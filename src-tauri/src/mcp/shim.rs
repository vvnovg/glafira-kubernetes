//! Режим `glafira-kubernetes mcp` (спец. §10.1): тот же бинарь без Tauri и окна. Находит
//! запущенную Glafira по `mcp.json` (при необходимости запускает её) и перекачивает байты
//! между stdin/stdout клиента MCP и сокетом приложения. В stdout — только протокол MCP.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use glafira_mcp_core::{BoxStream, endpoint, handshake};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const WAIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(250);

/// Окружение шима; в тестах подменяется.
pub trait ShimEnv {
    fn data_dir(&self) -> Option<PathBuf>;
    fn policy_allows(&self) -> bool;
    fn launch_app(&self) -> io::Result<()>;
}

#[derive(Debug)]
pub enum ShimError {
    NoDataDir,
    DisabledByPolicy,
    DisabledInSettings,
    Launch(io::Error),
    NotStarted,
    Handshake(handshake::HandshakeError),
}

impl fmt::Display for ShimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDataDir => write!(f, "Cannot find the Glafira data directory"),
            Self::DisabledByPolicy => write!(f, "MCP is disabled by your organization"),
            Self::DisabledInSettings => write!(
                f,
                "MCP is disabled in Glafira: turn it on in Settings → LLM assistants (MCP)"
            ),
            Self::Launch(e) => write!(f, "Cannot start Glafira: {e}"),
            Self::NotStarted => write!(
                f,
                "Glafira did not start the MCP server within {} s",
                WAIT.as_secs()
            ),
            Self::Handshake(e) => write!(f, "{e}"),
        }
    }
}

/// Проверки по порядку (как в Kafka G1), затем соединение с запущенной Glafira (или её запуск).
pub async fn connect(env: &dyn ShimEnv, wait: Duration) -> Result<BoxStream, ShimError> {
    let dir = env.data_dir().ok_or(ShimError::NoDataDir)?;
    if !env.policy_allows() {
        return Err(ShimError::DisabledByPolicy);
    }
    if !crate::settings::mcp_enabled_in(&dir) {
        return Err(ShimError::DisabledInSettings);
    }
    let deadline = tokio::time::Instant::now() + wait;
    let mut launched = false;
    loop {
        if let Some(f) = super::files::read(&dir)
            && let Ok(mut s) = endpoint::connect(&f.endpoint).await
        {
            handshake::client(&mut s, &f.token)
                .await
                .map_err(ShimError::Handshake)?;
            return Ok(s);
        }
        if !launched {
            env.launch_app().map_err(ShimError::Launch)?;
            launched = true;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ShimError::NotStarted);
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn copy_flushing<R, W>(mut r: R, mut w: W) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = r.read(&mut buf).await?;
        if n == 0 {
            let _ = w.shutdown().await;
            return Ok(());
        }
        w.write_all(&buf[..n]).await?;
        w.flush().await?;
    }
}

/// Перекачивает байты в обе стороны, пока одна из сторон не закончится.
pub async fn pump<I, O>(stream: BoxStream, input: I, output: O)
where
    I: AsyncRead + Unpin,
    O: AsyncWrite + Unpin,
{
    let (mut sr, mut sw) = tokio::io::split(stream);
    tokio::select! {
        _ = copy_flushing(input, &mut sw) => {}
        _ = copy_flushing(&mut sr, output) => {}
    }
}

/// `…/X.app/Contents/MacOS/<bin>` → `…/X.app`.
pub fn app_bundle(exe: &Path) -> Option<PathBuf> {
    let macos = exe.parent()?;
    let contents = macos.parent()?;
    let bundle = contents.parent()?;
    let ok = macos.file_name()? == "MacOS"
        && contents.file_name()? == "Contents"
        && bundle.extension()? == "app";
    ok.then(|| bundle.to_path_buf())
}

/// Запуск Glafira отдельным процессом, не наследующим stdio шима: иначе он держал бы pipe
/// клиента, и тот не увидел бы конец потока.
fn launch(exe: &Path) -> io::Result<()> {
    if cfg!(target_os = "macos")
        && let Some(bundle) = app_bundle(exe)
    {
        Command::new("open")
            .arg("-a")
            .arg(bundle)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        return Ok(());
    }
    // В AppImage current_exe() — во временной точке монтирования, которая исчезнет с шимом.
    let target = super::launch_path(exe, std::env::var_os("APPIMAGE"));
    let mut cmd = Command::new(target);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    cmd.spawn()?;
    Ok(())
}

struct RealEnv;

impl ShimEnv for RealEnv {
    fn data_dir(&self) -> Option<PathBuf> {
        super::files::data_dir()
    }

    fn policy_allows(&self) -> bool {
        crate::policy::sources::load().status.mcp_enabled
    }

    fn launch_app(&self) -> io::Result<()> {
        launch(&std::env::current_exe()?)
    }
}

/// Точка входа режима `mcp` (из `main`). Код возврата процесса.
pub fn run() -> i32 {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("glafira mcp: {e}");
            return 1;
        }
    };
    let code = rt.block_on(async {
        match connect(&RealEnv, WAIT).await {
            Ok(stream) => {
                pump(stream, tokio::io::stdin(), tokio::io::stdout()).await;
                0
            }
            Err(e) => {
                eprintln!("glafira mcp: {e}");
                1
            }
        }
    });
    // Чтение stdin в tokio блокирует поток; не ждём его при выходе.
    rt.shutdown_background();
    code
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::mcp::files::{self, McpFile};
    use crate::settings::SettingsStore;

    struct FakeEnv {
        dir: PathBuf,
        policy: bool,
        launches: AtomicU32,
        on_launch: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl ShimEnv for FakeEnv {
        fn data_dir(&self) -> Option<PathBuf> {
            Some(self.dir.clone())
        }
        fn policy_allows(&self) -> bool {
            self.policy
        }
        fn launch_app(&self) -> io::Result<()> {
            self.launches.fetch_add(1, Ordering::SeqCst);
            if let Some(f) = self.on_launch.lock().unwrap().take() {
                f();
            }
            Ok(())
        }
    }

    fn env(dir: &Path, policy: bool, mcp_enabled: bool) -> FakeEnv {
        SettingsStore::open(dir)
            .set_mcp_enabled(mcp_enabled)
            .unwrap();
        FakeEnv {
            dir: dir.to_path_buf(),
            policy,
            launches: AtomicU32::new(0),
            on_launch: Mutex::new(None),
        }
    }

    /// Как запуск приложения: через паузу слушает сокет, пишет mcp.json и принимает шим.
    fn fake_app(dir: PathBuf) -> Box<dyn FnOnce() + Send> {
        Box::new(move || {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let ep = endpoint::choose(&dir, crate::product::MCP_SERVER_NAME, "shimtest");
                let mut l = endpoint::Listener::bind(&ep).await.unwrap();
                let f = McpFile {
                    version: files::VERSION,
                    endpoint: ep,
                    token: "tok".into(),
                    pid: 1,
                };
                files::write(&dir, &f).unwrap();
                let mut s = l.accept().await.unwrap();
                handshake::server(&mut s, "tok").await.unwrap();
                s.write_all(b"from-app").await.unwrap();
                tokio::time::sleep(Duration::from_millis(200)).await;
            });
        })
    }

    #[tokio::test]
    async fn refuses_when_policy_or_settings_disable_mcp() {
        let dir = tempfile::tempdir().unwrap();
        let e = env(dir.path(), false, true);
        assert!(matches!(
            connect(&e, WAIT).await,
            Err(ShimError::DisabledByPolicy)
        ));
        // Формулировка политики — как в приложении («вашей организацией»).
        assert_eq!(
            ShimError::DisabledByPolicy.to_string(),
            "MCP is disabled by your organization"
        );
        let e = env(dir.path(), true, false);
        assert!(matches!(
            connect(&e, WAIT).await,
            Err(ShimError::DisabledInSettings)
        ));
        assert_eq!(
            e.launches.load(Ordering::SeqCst),
            0,
            "выключенный MCP не запускает Glafira"
        );
    }

    #[tokio::test]
    async fn launches_glafira_once_and_waits_for_the_socket() {
        let dir = tempfile::tempdir().unwrap();
        let e = env(dir.path(), true, true);
        *e.on_launch.lock().unwrap() = Some(fake_app(dir.path().to_path_buf()));
        let mut s = connect(&e, Duration::from_secs(5)).await.unwrap();
        assert_eq!(e.launches.load(Ordering::SeqCst), 1);
        let mut buf = [0u8; 8];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"from-app");
    }

    #[tokio::test]
    async fn gives_up_when_glafira_does_not_start() {
        let dir = tempfile::tempdir().unwrap();
        let e = env(dir.path(), true, true);
        let r = connect(&e, Duration::from_millis(600)).await;
        assert!(matches!(r, Err(ShimError::NotStarted)));
        assert_eq!(e.launches.load(Ordering::SeqCst), 1);
        assert!(ShimError::NotStarted.to_string().contains("30 s"));
    }

    #[tokio::test]
    async fn pump_carries_bytes_both_ways() {
        let (app_side, shim_side) = tokio::io::duplex(1024);
        let (mut stdin_writer, stdin) = tokio::io::duplex(1024);
        let app = tokio::spawn(async move {
            let mut app_side = app_side;
            let mut buf = [0u8; 5];
            app_side.read_exact(&mut buf).await.unwrap();
            app_side.write_all(b"HELLO").await.unwrap();
            buf
        });
        stdin_writer.write_all(b"hello").await.unwrap();
        let mut out = Vec::new();
        pump(Box::new(shim_side), stdin, &mut out).await;
        assert_eq!(&app.await.unwrap(), b"hello");
        assert_eq!(out, b"HELLO");
    }

    #[test]
    fn app_bundle_is_found_only_inside_a_bundle() {
        assert_eq!(
            app_bundle(Path::new(
                "/Applications/Glafira for Kubernetes.app/Contents/MacOS/glafira-kubernetes"
            )),
            Some(PathBuf::from("/Applications/Glafira for Kubernetes.app"))
        );
        assert_eq!(app_bundle(Path::new("/usr/bin/glafira-kubernetes")), None);
    }
}
