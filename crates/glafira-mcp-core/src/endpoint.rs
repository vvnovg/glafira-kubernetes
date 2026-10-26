//! Где слушает приложение и как к нему подключается шим (спец. §10.1): Unix socket на macOS
//! и Linux, named pipe на Windows. Реальный адрес записывается в `mcp.json`. Имя продукта
//! (`glafira-kubernetes`) — в запасном имени сокета и в имени pipe.

use std::path::Path;

use tokio::io::{AsyncRead, AsyncWrite};

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
pub type BoxStream = Box<dyn Stream>;

/// Путь Unix socket или имя named pipe.
pub type Endpoint = String;

pub const SOCKET_NAME: &str = "mcp.sock";
/// Предел `sun_path` — 104 байта на macOS и 108 на Linux; берём с запасом.
pub const MAX_SOCKET_PATH: usize = 100;

/// Адрес для нового сервера; `product` — имя продукта, `suffix` — случайные hex-символы (имя
/// pipe, запасное имя сокета).
#[cfg(unix)]
pub fn choose(data_dir: &Path, product: &str, suffix: &str) -> Endpoint {
    let p = data_dir.join(SOCKET_NAME);
    if p.as_os_str().len() <= MAX_SOCKET_PATH {
        return p.to_string_lossy().into_owned();
    }
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .filter(|d| d.is_dir())
        .unwrap_or_else(std::env::temp_dir);
    base.join(format!("{product}-mcp-{suffix}.sock"))
        .to_string_lossy()
        .into_owned()
}

#[cfg(windows)]
pub fn choose(_data_dir: &Path, product: &str, suffix: &str) -> Endpoint {
    format!(r"\\.\pipe\{product}-mcp-{suffix}")
}

pub use imp::{Listener, connect};

#[cfg(unix)]
mod imp {
    use std::io;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use tokio::net::{UnixListener, UnixStream};

    use super::{BoxStream, Endpoint};

    pub struct Listener {
        inner: UnixListener,
        path: PathBuf,
    }

    impl Listener {
        /// Живой сокет по этому пути — ошибка `AddrInUse`; оставшийся после сбоя файл удаляется.
        pub async fn bind(ep: &Endpoint) -> io::Result<Self> {
            let path = PathBuf::from(ep);
            if path.exists() {
                if UnixStream::connect(&path).await.is_ok() {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "the MCP endpoint is in use",
                    ));
                }
                let _ = std::fs::remove_file(&path);
            }
            let inner = UnixListener::bind(&path)?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            Ok(Self { inner, path })
        }

        pub async fn accept(&mut self) -> io::Result<BoxStream> {
            let (s, _) = self.inner.accept().await?;
            Ok(Box::new(s))
        }
    }

    impl Drop for Listener {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    pub async fn connect(ep: &Endpoint) -> io::Result<BoxStream> {
        Ok(Box::new(UnixStream::connect(ep).await?))
    }
}

#[cfg(windows)]
mod imp {
    use std::io;
    use std::time::Duration;

    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};

    use super::{BoxStream, Endpoint};

    /// ERROR_PIPE_BUSY: все экземпляры pipe заняты, сервер вот-вот создаст следующий.
    const ERROR_PIPE_BUSY: i32 = 231;

    pub struct Listener {
        name: String,
        next: NamedPipeServer,
    }

    impl Listener {
        pub async fn bind(ep: &Endpoint) -> io::Result<Self> {
            let next = ServerOptions::new()
                .first_pipe_instance(true)
                .reject_remote_clients(true)
                .create(ep)?;
            Ok(Self {
                name: ep.clone(),
                next,
            })
        }

        fn create(&self) -> io::Result<NamedPipeServer> {
            ServerOptions::new()
                .reject_remote_clients(true)
                .create(&self.name)
        }

        pub async fn accept(&mut self) -> io::Result<BoxStream> {
            if let Err(e) = self.next.connect().await {
                // Экземпляр после неудачного connect (ERROR_NO_DATA: клиент уже отключился и т. п.)
                // больше не примет соединение — иначе каждый следующий accept падал бы сразу.
                self.next = self.create()?;
                return Err(e);
            }
            let fresh = self.create()?;
            Ok(Box::new(std::mem::replace(&mut self.next, fresh)))
        }
    }

    pub async fn connect(ep: &Endpoint) -> io::Result<BoxStream> {
        for _ in 0..20 {
            match ClientOptions::new().open(ep) {
                Ok(c) => return Ok(Box::new(c)),
                Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                Err(e) => return Err(e),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "the MCP pipe is busy",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PRODUCT: &str = "glafira-kubernetes";

    /// Уникальный адрес на тест: имя временного каталога различает и сокеты, и pipe.
    fn temp_endpoint(dir: &Path) -> Endpoint {
        choose(dir, PRODUCT, &dir.file_name().unwrap().to_string_lossy())
    }

    #[tokio::test]
    async fn connects_and_carries_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let ep = temp_endpoint(dir.path());
        let mut l = Listener::bind(&ep).await.unwrap();
        let srv = tokio::spawn(async move {
            let mut s = l.accept().await.unwrap();
            let mut buf = [0u8; 5];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(&buf).await.unwrap();
        });
        let mut c = connect(&ep).await.unwrap();
        c.write_all(b"hello").await.unwrap();
        let mut back = [0u8; 5];
        c.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"hello");
        srv.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn socket_is_private_stale_file_is_replaced_and_removed_on_drop() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let ep = choose(dir.path(), PRODUCT, "x");
        std::fs::write(&ep, b"stale").unwrap();
        let l = Listener::bind(&ep).await.unwrap();
        let mode = std::fs::metadata(&ep).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let e = Listener::bind(&ep).await.err().unwrap();
        assert_eq!(e.kind(), std::io::ErrorKind::AddrInUse);
        drop(l);
        assert!(!Path::new(&ep).exists());
    }

    #[cfg(unix)]
    #[test]
    fn long_data_dir_falls_back_to_short_socket() {
        let long = Path::new("/").join("d".repeat(120));
        let ep = choose(&long, PRODUCT, "abcd1234");
        assert!(ep.ends_with("glafira-kubernetes-mcp-abcd1234.sock"), "{ep}");
        assert!(!ep.starts_with(long.to_str().unwrap()));
        let short = choose(Path::new("/tmp/x"), PRODUCT, "abcd1234");
        assert_eq!(short, "/tmp/x/mcp.sock");
    }
}
