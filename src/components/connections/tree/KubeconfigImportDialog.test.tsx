import { useQuery } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ComponentProps } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { useConnectionEpoch } from "../../../state/connectionEpoch";
import { renderWithClient } from "../../../test/render";

vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn() }));
vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return {
    ...orig,
    api: {
      connect: vi.fn(),
      listConnections: vi.fn(),
      listFolders: vi.fn(),
      previewKubeconfigImport: vi.fn(),
      applyKubeconfigImport: vi.fn(),
    },
  };
});

import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { api } from "../../../api/client";
import type { ConnectionProfile, KubeconfigPreview, KubeContext } from "../../../api/types";
import { KubeconfigImportDialog } from "./KubeconfigImportDialog";

const ctx = (name: string, extra: Partial<KubeContext> = {}): KubeContext => ({
  name,
  cluster: "c",
  server: "https://api.dev:6443",
  user: "alice",
  namespace: null,
  auth: "token",
  insecure: false,
  current: false,
  exec: null,
  oidc: null,
  execUnsupported: null,
  unsupported: null,
  existingId: null,
  ...extra,
});

const PREVIEW: KubeconfigPreview = {
  path: "/home/u/.kube/config",
  contexts: [
    ctx("dev", { current: true, namespace: "team-a" }),
    ctx("lab", { insecure: true, existingId: "old" }),
    ctx("evil", {
      auth: "exec",
      unsupported: { key: "errors.exec.notAllowed", params: { command: "sh" }, text: "not allowed" },
    }),
  ],
};

/** Контекст с exec-плагином: `launchFingerprint` — отпечаток строки запуска, как его считает Rust. */
const execCtx = (name: string, fingerprint: string, extra: Partial<KubeContext> = {}): KubeContext =>
  ctx(name, {
    auth: "exec",
    exec: {
      command: "/usr/local/bin/aws",
      args: ["--region", "eu-west-1", "eks", "get-token", "--cluster-name", name],
      env: [{ name: "AWS_PROFILE", value: "prod" }],
      apiVersion: "client.authentication.k8s.io/v1beta1",
      provideClusterInfo: false,
      approved: null,
    },
    launchFingerprint: fingerprint,
    ...extra,
  });

const ALLOW = "Разрешить Glafira запускать эту команду";

/** Контекст kubeconfig от Loginapp DropApp: `kubectl oidc-login get-token` с аргументами Dex. */
const oidcCtx = (name: string, extra: Partial<KubeContext> = {}): KubeContext =>
  ctx(name, {
    auth: "oidc",
    user: "oidc",
    oidc: {
      issuerUrl: "https://auth.c1.example.com",
      clientId: "dapp",
      scopes: ["openid", "groups"],
      hasClientSecret: true,
      issuerCaWarning: null,
    },
    exec: {
      command: "/usr/local/bin/kubectl",
      args: ["oidc-login", "get-token", "--oidc-issuer-url=https://auth.c1.example.com", "--oidc-client-secret=***"],
      env: [],
      apiVersion: "client.authentication.k8s.io/v1beta1",
      provideClusterInfo: false,
      approved: null,
    },
    launchFingerprint: `fp-${name}`,
    ...extra,
  });

/** Диалог рядом с подписчиком на список подключений: после импорта список должен перечитаться. */
function Mounted(props: ComponentProps<typeof KubeconfigImportDialog>) {
  useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  return <KubeconfigImportDialog {...props} />;
}

/** Открытая вкладка подключения «old»: её данные подписаны на ключ `["connection", id]`. */
function OpenTab() {
  const q = useQuery({ queryKey: ["connection", "old"], queryFn: () => api.connect("old", false) });
  return <output aria-label="Вкладка">{q.data === undefined ? "…" : String(q.data)}</output>;
}

beforeEach(() => {
  vi.clearAllMocks();
  useConnectionEpoch.setState({ epochs: {} });
  vi.mocked(api.listConnections).mockResolvedValue([]);
  vi.mocked(api.listFolders).mockResolvedValue([{ id: "f", name: "Kubeconfig", parentId: null, order: 0 }]);
  vi.mocked(api.previewKubeconfigImport).mockResolvedValue(PREVIEW);
});

describe("KubeconfigImportDialog", () => {
  it("показывает контексты, отмечает текущий и объясняет неподдерживаемые", async () => {
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    expect(await screen.findByText("/home/u/.kube/config")).toBeInTheDocument();
    expect(api.previewKubeconfigImport).toHaveBeenCalledWith(null);
    expect(screen.getByRole("checkbox", { name: "dev" })).toBeChecked();
    expect(screen.getByRole("checkbox", { name: "lab" })).not.toBeChecked();
    expect(screen.getByRole("checkbox", { name: "evil" })).toBeDisabled();
    expect(screen.getByText("«sh» нет в списке разрешённых exec-плагинов")).toBeInTheDocument();
    expect(screen.getByText(/без проверки сертификата/)).toBeInTheDocument();
    expect(screen.getByText(/обновит существующее подключение/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Импортировать 1 контекст" })).toBeEnabled();
  });

  it("импортирует выбранные контексты в папку", async () => {
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([]);
    const onImported = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={onImported} />);
    await user.click(await screen.findByRole("checkbox", { name: "lab" }));
    await user.selectOptions(screen.getByLabelText("Папка для новых подключений"), "f");
    await user.click(screen.getByRole("button", { name: "Импортировать 2 контекста" }));
    await waitFor(() => expect(onImported).toHaveBeenCalledWith([]));
    expect(api.applyKubeconfigImport).toHaveBeenCalledWith("/home/u/.kube/config", ["dev", "lab"], "f", [], []);
  });

  it("exec-плагин: строка запуска и отдельное разрешение перед импортом", async () => {
    vi.mocked(api.previewKubeconfigImport).mockResolvedValue({
      path: "/home/u/.kube/config",
      contexts: [
        ctx("eks", {
          auth: "exec",
          current: true,
          exec: {
            command: "/usr/local/bin/aws",
            args: ["--region", "eu-west-1", "eks", "get-token", "--cluster-name", "prod"],
            env: [{ name: "AWS_PROFILE", value: "prod" }],
            apiVersion: "client.authentication.k8s.io/v1beta1",
            provideClusterInfo: false,
            approved: null,
          },
          launchFingerprint: "fp-eks",
        }),
      ],
    });
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([]);
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    expect(await screen.findByText(/alice · exec-плагин/)).toBeInTheDocument();
    expect(screen.getByLabelText("Строка запуска")).toHaveTextContent(
      "AWS_PROFILE=prod /usr/local/bin/aws --region eu-west-1 eks get-token --cluster-name prod",
    );
    const importButton = screen.getByRole("button", { name: "Импортировать 1 контекст" });
    expect(importButton).toBeDisabled();
    expect(screen.getByText("Разрешите запуск выбранных exec-плагинов, чтобы импортировать их")).toBeInTheDocument();
    await user.click(screen.getByRole("checkbox", { name: ALLOW }));
    await user.click(importButton);
    // Подтверждение — отпечаток показанной строки, а не имя контекста.
    expect(api.applyKubeconfigImport).toHaveBeenCalledWith("/home/u/.kube/config", ["eks"], null, ["fp-eks"], []);
  });

  it("exec-плагин: отпечатки уходят только у выбранных и разрешённых контекстов", async () => {
    vi.mocked(api.previewKubeconfigImport).mockResolvedValue({
      path: "/home/u/.kube/config",
      contexts: [execCtx("a", "fp-a", { current: true }), execCtx("b", "fp-b"), ctx("c")],
    });
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([]);
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    await user.click(await screen.findByRole("checkbox", { name: "b" }));
    await user.click(screen.getByRole("checkbox", { name: "c" }));
    const [allowA, allowB] = screen.getAllByRole("checkbox", { name: ALLOW });
    await user.click(allowA);
    await user.click(allowB);
    // Контекст b снят с выбора: его отпечаток не уходит, хотя галочка у него стояла.
    await user.click(screen.getByRole("checkbox", { name: "b" }));
    await user.click(screen.getByRole("button", { name: "Импортировать 2 контекста" }));
    expect(api.applyKubeconfigImport).toHaveBeenCalledWith("/home/u/.kube/config", ["a", "c"], null, ["fp-a"], []);
  });

  it("exec-плагин: после перечитывания с другой строкой запуска галочка не переносится", async () => {
    const first = { path: "/home/u/.kube/config", contexts: [execCtx("eks", "fp-1", { current: true })] };
    const changed = execCtx("eks", "fp-2", { current: true });
    if (changed.exec) changed.exec.args = ["--evil"];
    vi.mocked(api.previewKubeconfigImport)
      .mockResolvedValueOnce(first)
      .mockResolvedValue({ path: "/home/u/.kube/config", contexts: [changed] });
    vi.mocked(api.applyKubeconfigImport)
      .mockRejectedValueOnce({ code: "INTERNAL", message: "Keychain is locked", retryable: false })
      .mockResolvedValue([]);
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    await user.click(await screen.findByRole("checkbox", { name: ALLOW }));
    await user.click(screen.getByRole("button", { name: "Импортировать 1 контекст" }));
    // Сбой обновил предпросмотр: файл изменился, показана новая строка запуска.
    expect(await screen.findByText(/--evil/)).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: ALLOW })).not.toBeChecked();
    expect(screen.getByRole("button", { name: "Импортировать 1 контекст" })).toBeDisabled();
    await user.click(screen.getByRole("checkbox", { name: ALLOW }));
    await user.click(screen.getByRole("button", { name: "Импортировать 1 контекст" }));
    expect(api.applyKubeconfigImport).toHaveBeenLastCalledWith("/home/u/.kube/config", ["eks"], null, ["fp-2"], []);
  });

  it("OIDC: по умолчанию вход через браузер, exec-плагин — по выбору и с разрешением", async () => {
    vi.mocked(api.previewKubeconfigImport).mockResolvedValue({
      path: "/home/u/.kube/config",
      contexts: [
        oidcCtx("dropapp", { current: true }),
        oidcCtx("no-kubectl", {
          exec: null,
          launchFingerprint: undefined,
          execUnsupported: { key: "errors.exec.badApiVersion", params: { version: "client.authentication.io/v1beta1" }, text: "x" },
        }),
      ],
    });
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([]);
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    const [oidcA, oidcB] = await screen.findAllByRole("radio", { name: "Вход через браузер (OIDC)" });
    const [execA, execB] = screen.getAllByRole("radio", { name: "exec-плагин" });
    expect(oidcA).toBeChecked();
    expect(oidcB).toBeChecked();
    expect(execA).not.toBeChecked();
    // Exec-плагин недоступен — его причина видна, вход через OIDC остаётся.
    expect(execB).toBeDisabled();
    expect(screen.getByText(/client\.authentication\.io\/v1beta1/)).toBeInTheDocument();
    expect(screen.getAllByText(/https:\/\/auth\.c1\.example\.com · dapp/)).toHaveLength(2);
    // При входе через OIDC строку запуска не разрешают.
    expect(screen.queryByRole("checkbox", { name: ALLOW })).not.toBeInTheDocument();
    const importButton = screen.getByRole("button", { name: "Импортировать 1 контекст" });
    await user.click(importButton);
    expect(api.applyKubeconfigImport).toHaveBeenLastCalledWith("/home/u/.kube/config", ["dropapp"], null, [], []);

    await user.click(execA);
    expect(screen.getByLabelText("Строка запуска")).toHaveTextContent("kubectl oidc-login get-token");
    expect(importButton).toBeDisabled();
    await user.click(screen.getByRole("checkbox", { name: ALLOW }));
    await user.click(importButton);
    expect(api.applyKubeconfigImport).toHaveBeenLastCalledWith("/home/u/.kube/config", ["dropapp"], null, ["fp-dropapp"], ["dropapp"]);
  });

  it("OIDC: нечитаемый CA издателя — предупреждение с путём, контекст импортируется", async () => {
    const base = oidcCtx("dropapp", { current: true });
    vi.mocked(api.previewKubeconfigImport).mockResolvedValue({
      path: "/home/u/.kube/config",
      contexts: [
        {
          ...base,
          oidc: {
            ...base.oidc!,
            issuerCaWarning: {
              key: "errors.kubeconfig.issuerCaUnreadable",
              params: { path: "/home/u/dex-ca.crt", error: "No such file or directory (os error 2)" },
              text: "x",
            },
          },
        },
      ],
    });
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([]);
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    expect(await screen.findByText(/Не удалось прочитать CA издателя OIDC \/home\/u\/dex-ca\.crt/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Импортировать 1 контекст" }));
    expect(api.applyKubeconfigImport).toHaveBeenLastCalledWith("/home/u/.kube/config", ["dropapp"], null, [], []);
  });

  it("выбор другого файла перечитывает контексты", async () => {
    vi.mocked(openDialog).mockResolvedValue("/tmp/other.yaml");
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    await user.click(await screen.findByRole("button", { name: "Выбрать файл…" }));
    await waitFor(() => expect(api.previewKubeconfigImport).toHaveBeenCalledWith("/tmp/other.yaml"));
  });

  it("ошибка чтения файла и Esc закрывает", async () => {
    vi.mocked(api.previewKubeconfigImport).mockRejectedValue({
      code: "INVALID_INPUT",
      key: "errors.kubeconfig.noDefault",
      message: "x",
      retryable: false,
    });
    const onClose = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={onClose} onImported={() => {}} />);
    expect(await screen.findByText("Kubeconfig по умолчанию не найден — выберите файл")).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalled();
  });

  it("после импорта сбрасывает кэш обновлённых подключений и перечитывает список", async () => {
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([{ id: "new" }, { id: "old" }] as ConnectionProfile[]);
    let cached: unknown[] = [];
    const onImported = vi.fn(() => {
      cached = [rendered.client.getQueryData(["connection", "old"]), rendered.client.getQueryData(["namespaces", "new"])];
    });
    const user = userEvent.setup();
    const rendered = renderWithClient(<Mounted initialTargetId={null} onClose={() => {}} onImported={onImported} />);
    rendered.client.setQueryData(["connection", "old"], { stale: true });
    rendered.client.setQueryData(["namespaces", "new"], ["a"]);
    rendered.client.setQueryData(["connection", "other"], { keep: true });
    await user.click(await screen.findByRole("checkbox", { name: "lab" }));
    await user.click(screen.getByRole("button", { name: "Импортировать 2 контекста" }));
    await waitFor(() => expect(onImported).toHaveBeenCalled());
    // К моменту onImported открытая вкладка подключения уже не видит прежний адрес и вход.
    expect(cached).toEqual([undefined, undefined]);
    expect(rendered.client.getQueryData(["connection", "other"])).toEqual({ keep: true });
    expect(api.listConnections).toHaveBeenCalledTimes(2);
    // Диалог закрывается: файл заново не читается.
    expect(api.previewKubeconfigImport).toHaveBeenCalledTimes(1);
  });

  it("после импорта открытая вкладка подключения перезапрашивает данные", async () => {
    vi.mocked(api.connect).mockResolvedValueOnce("сеанс истёк" as never).mockResolvedValue("подключено" as never);
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([{ id: "old" }] as ConnectionProfile[]);
    const user = userEvent.setup();
    renderWithClient(
      <>
        <OpenTab />
        <Mounted initialTargetId={null} onClose={() => {}} onImported={() => {}} />
      </>,
    );
    await waitFor(() => expect(screen.getByLabelText("Вкладка")).toHaveTextContent("сеанс истёк"));
    await user.click(await screen.findByRole("checkbox", { name: "lab" }));
    await user.click(screen.getByRole("button", { name: "Импортировать 2 контекста" }));
    // Подписчик ключа `["connection", "old"]` получает свежие данные без повторного открытия вкладки.
    await waitFor(() => expect(screen.getByLabelText("Вкладка")).toHaveTextContent("подключено"));
    expect(api.connect).toHaveBeenCalledTimes(2);
  });

  it("после импорта перезапускает watch и логи существующих подключений, но не новых", async () => {
    // «dev» — новое подключение (id «new»), «lab» обновляет существующее «old»: сессии есть только у него.
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([{ id: "new" }, { id: "old" }] as ConnectionProfile[]);
    const onImported = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<Mounted initialTargetId={null} onClose={() => {}} onImported={onImported} />);
    await user.click(await screen.findByRole("checkbox", { name: "lab" }));
    await user.click(screen.getByRole("button", { name: "Импортировать 2 контекста" }));
    await waitFor(() => expect(onImported).toHaveBeenCalled());
    expect(useConnectionEpoch.getState().epochs).toEqual({ old: 1 });
  });

  it("импорт только новых контекстов сессий не перезапускает", async () => {
    vi.mocked(api.applyKubeconfigImport).mockResolvedValue([{ id: "new" }] as ConnectionProfile[]);
    const onImported = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<Mounted initialTargetId={null} onClose={() => {}} onImported={onImported} />);
    await user.click(await screen.findByRole("button", { name: "Импортировать 1 контекст" }));
    await waitFor(() => expect(onImported).toHaveBeenCalled());
    expect(useConnectionEpoch.getState().epochs).toEqual({});
  });

  it("сбой сохранения: часть контекстов могла записаться, сессии существующих подключений перезапускаются", async () => {
    vi.mocked(api.applyKubeconfigImport).mockRejectedValue({ code: "INTERNAL", message: "Keychain is locked", retryable: false });
    const user = userEvent.setup();
    renderWithClient(<Mounted initialTargetId={null} onClose={() => {}} onImported={vi.fn()} />);
    await user.click(await screen.findByRole("checkbox", { name: "lab" }));
    await user.click(screen.getByRole("button", { name: "Импортировать 2 контекста" }));
    expect(await screen.findByText("Keychain is locked")).toBeInTheDocument();
    expect(useConnectionEpoch.getState().epochs).toEqual({ old: 1 });
  });

  it("сбой сохранения: ошибка, диалог открыт, списки перечитаны", async () => {
    vi.mocked(api.applyKubeconfigImport).mockRejectedValue({ code: "INTERNAL", message: "Keychain is locked", retryable: false });
    const onImported = vi.fn();
    const onClose = vi.fn();
    const user = userEvent.setup();
    const { client } = renderWithClient(<Mounted initialTargetId={null} onClose={onClose} onImported={onImported} />);
    client.setQueryData(["connection", "old"], { stale: true });
    await user.click(await screen.findByRole("checkbox", { name: "lab" }));
    await waitFor(() => expect(api.listConnections).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole("button", { name: "Импортировать 2 контекста" }));
    expect(await screen.findByText("Keychain is locked")).toBeInTheDocument();
    // Часть контекстов могла сохраниться: список подключений и пометки «обновит» перечитываются.
    await waitFor(() => expect(api.listConnections).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(api.previewKubeconfigImport).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.getByRole("button", { name: "Импортировать 2 контекста" })).toBeEnabled());
    expect(client.getQueryData(["connection", "old"])).toBeUndefined();
    expect(onImported).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("смена файла сбрасывает выбор и разрешения exec-плагинов", async () => {
    const first = { path: "/home/u/.kube/config", contexts: [execCtx("eks", "fp-eks", { current: true }), ctx("dev")] };
    const second = { path: "/tmp/other.yaml", contexts: [execCtx("eks", "fp-eks", { current: true }), ctx("dev")] };
    vi.mocked(api.previewKubeconfigImport).mockImplementation(async (p) => (p ? second : first));
    vi.mocked(openDialog).mockResolvedValue("/tmp/other.yaml");
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    await user.click(await screen.findByRole("checkbox", { name: "dev" }));
    await user.click(screen.getByRole("checkbox", { name: ALLOW }));
    expect(screen.getByRole("button", { name: "Импортировать 2 контекста" })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: "Выбрать файл…" }));
    expect(await screen.findByText("/tmp/other.yaml")).toBeInTheDocument();
    // Тот же отпечаток во втором файле, но прежняя галочка и ручной выбор не переносятся.
    expect(screen.getByRole("checkbox", { name: "eks" })).toBeChecked();
    expect(screen.getByRole("checkbox", { name: "dev" })).not.toBeChecked();
    expect(screen.getByRole("checkbox", { name: ALLOW })).not.toBeChecked();
    expect(screen.getByRole("button", { name: "Импортировать 1 контекст" })).toBeDisabled();
  });

  it("ошибку выбора файла показывает в диалоге", async () => {
    vi.mocked(openDialog).mockRejectedValue(new Error("Dialog is unavailable"));
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    await user.click(await screen.findByRole("button", { name: "Выбрать файл…" }));
    expect(await screen.findByText("Dialog is unavailable")).toBeInTheDocument();
  });

  it("ошибку чтения файла можно повторить", async () => {
    vi.mocked(api.previewKubeconfigImport)
      .mockRejectedValueOnce({ code: "INVALID_INPUT", key: "errors.kubeconfig.noDefault", message: "x", retryable: false })
      .mockResolvedValue(PREVIEW);
    const user = userEvent.setup();
    renderWithClient(<KubeconfigImportDialog initialTargetId={null} onClose={() => {}} onImported={() => {}} />);
    await user.click(await screen.findByRole("button", { name: "Повторить" }));
    expect(await screen.findByRole("checkbox", { name: "dev" })).toBeChecked();
    expect(api.previewKubeconfigImport).toHaveBeenCalledTimes(2);
  });
});
