import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../../test/render";

vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn() }));
vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return {
    ...orig,
    api: { listFolders: vi.fn(), previewKubeconfigImport: vi.fn(), applyKubeconfigImport: vi.fn() },
  };
});

import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { api } from "../../../api/client";
import type { KubeconfigPreview, KubeContext } from "../../../api/types";
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

beforeEach(() => {
  vi.clearAllMocks();
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
    expect(api.applyKubeconfigImport).toHaveBeenCalledWith("/home/u/.kube/config", ["dev", "lab"], "f", []);
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
    await user.click(screen.getByRole("checkbox", { name: "Разрешить Glafira запускать эту команду" }));
    await user.click(importButton);
    expect(api.applyKubeconfigImport).toHaveBeenCalledWith("/home/u/.kube/config", ["eks"], null, ["eks"]);
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
});
