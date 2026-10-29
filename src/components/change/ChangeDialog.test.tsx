import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { listConnections: vi.fn(), previewChange: vi.fn(), applyChange: vi.fn(), getNamespaceAccess: vi.fn() } };
});

import { api } from "../../api/client";
import type { AppError, Change, ChangePreview, ConnectionProfile, WorkloadDetails } from "../../api/types";
import { workloadRow } from "../../test/objects";
import { newProfile } from "../connections/profile";
import { WorkloadActions } from "./ChangeActions";
import { ChangeDialog } from "./ChangeDialog";

const dev: ConnectionProfile = { ...newProfile(), id: "c1", name: "dev", environment: "dev" };
const web = { kind: "Deployment", name: "web" } as const;
const resources: Change = {
  kind: "setResources",
  namespace: "team-a",
  workload: web,
  containers: [
    { name: "app", requests: {}, limits: {} },
    { name: "proxy", requests: {}, limits: {} },
  ],
};
const preview = (change: Change, p: Partial<ChangePreview> = {}): ChangePreview => ({
  change,
  version: "4",
  diff: [{ container: "app", field: "requests.cpu", before: "500m", after: "250m" }],
  warnings: [],
  dryRun: { status: "passed" },
  confirmPodName: false,
  ...p,
});

beforeEach(() => {
  vi.mocked(api.listConnections).mockResolvedValue([dev]);
  vi.mocked(api.previewChange).mockReset();
  vi.mocked(api.applyChange).mockReset();
});

describe("ChangeDialog", () => {
  it("ресурсы: отправляются только заполненные поля, «было → станет», применение с версией предпросмотра", async () => {
    vi.mocked(api.previewChange).mockImplementation(async (_id, c) => preview(c));
    vi.mocked(api.applyChange).mockImplementation(async (_id, c) => c);
    const onClose = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<ChangeDialog connectionId="c1" initial={resources} hints={{ app: { requests: { cpu: "500m" }, limits: {} } }} onClose={onClose} />);
    expect(screen.getByRole("dialog", { name: "Ресурсы контейнеров: Deployment web" })).toBeInTheDocument();
    expect(screen.getByLabelText("app CPU request")).toHaveAttribute("placeholder", "сейчас 500m");
    await user.type(screen.getByLabelText("app CPU request"), "250m");
    await user.click(screen.getByRole("button", { name: "Убрать значение: app CPU limit" }));
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    const sent: Change = {
      kind: "setResources",
      namespace: "team-a",
      workload: web,
      containers: [{ name: "app", requests: { cpu: "250m" }, limits: { cpu: null } }],
    };
    expect(api.previewChange).toHaveBeenCalledWith("c1", sent);
    expect(await screen.findByText("500m")).toBeInTheDocument();
    expect(screen.getByText("API-сервер принял изменение (dry-run)", { exact: false })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Применить" }));
    await waitFor(() => expect(onClose).toHaveBeenCalled());
    expect(api.applyChange).toHaveBeenCalledWith("c1", sent, "4", null, null, null);
  });

  it("правка формы сбрасывает предпросмотр; CONFLICT — «Обновить и повторить»", async () => {
    const conflict: AppError = { code: "CONFLICT", key: "errors.ops.changed", message: "The object has changed", retryable: false };
    vi.mocked(api.previewChange).mockImplementation(async (_id, c) => preview(c));
    vi.mocked(api.applyChange).mockRejectedValue(conflict);
    const user = userEvent.setup();
    const scale: Change = { kind: "scale", namespace: "team-a", workload: web, replicas: 2 };
    renderWithClient(<ChangeDialog connectionId="c1" initial={scale} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    await user.click(await screen.findByRole("button", { name: "Применить" }));
    await user.click(await screen.findByRole("button", { name: "Обновить и повторить" }));
    expect(screen.queryByRole("button", { name: "Применить" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    expect(api.previewChange).toHaveBeenCalledTimes(2);
    await user.clear(screen.getByLabelText("Реплики"));
    await user.type(screen.getByLabelText("Реплики"), "5");
    expect(screen.getByRole("button", { name: "Проверить" })).toBeInTheDocument();
  });

  it("под без контроллера удаляется только с его именем; prod — с именем подключения", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([{ ...dev, name: "prod-1", environment: "prod" }]);
    const del: Change = { kind: "deletePod", namespace: "team-a", pod: "solo" };
    vi.mocked(api.previewChange).mockResolvedValue(
      preview(del, { version: "u1", diff: [{ container: null, field: "pod", before: "solo", after: null }], warnings: [{ code: "orphan" }], confirmPodName: true }),
    );
    vi.mocked(api.applyChange).mockResolvedValue(del);
    const user = userEvent.setup();
    renderWithClient(<ChangeDialog connectionId="c1" initial={del} onClose={() => {}} />);
    await user.click(await screen.findByRole("button", { name: "Проверить" }));
    expect(await screen.findByText("У пода нет контроллера: заново его никто не создаст")).toBeInTheDocument();
    const button = screen.getByRole("button", { name: "Удалить" });
    await user.type(screen.getByLabelText("Подтверждение: имя пода"), "solo");
    expect(button).toBeDisabled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(button);
    await waitFor(() => expect(api.applyChange).toHaveBeenCalledWith("c1", del, "u1", "prod-1", "solo", null));
  });

  it("отказ dry-run виден до подтверждения, применить нельзя", async () => {
    const denied: AppError = { code: "ADMISSION_DENIED", key: "errors.ops.admissionDenied", params: { message: "quota exceeded" }, message: "rejected", retryable: false };
    const restart: Change = { kind: "rolloutRestart", namespace: "team-a", workload: web };
    vi.mocked(api.previewChange).mockResolvedValue(preview(restart, { dryRun: { status: "rejected", error: denied } }));
    const user = userEvent.setup();
    renderWithClient(<ChangeDialog connectionId="c1" initial={restart} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    expect(await screen.findByText("API-сервер отклонил изменение")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Применить" })).not.toBeInTheDocument();
  });
});

describe("WorkloadActions", () => {
  const details: WorkloadDetails = {
    workload: workloadRow("web", { desired: 2 }),
    conditions: [],
    selector: null,
    strategy: null,
    containers: [],
    labels: {},
    annotations: {},
    activeJobs: [],
    hpa: { data: { name: "web-hpa", minReplicas: 2, maxReplicas: 6, currentReplicas: 2, desiredReplicas: 2, metrics: [], conditions: [] } },
    pdb: { data: null },
    history: null,
  };
  const verbs = (patch: boolean) => ({ get: true, list: true, watch: true, patch, delete: false, create: false });

  it("без прав кнопка неактивна с подсказкой; read-only — все неактивны", async () => {
    vi.mocked(api.getNamespaceAccess).mockResolvedValue({
      resources: { "deployments.apps": verbs(true), "deployments/scale.apps": verbs(false), "horizontalpodautoscalers.autoscaling": verbs(true) },
      incomplete: false,
    });
    const { unmount } = renderWithClient(<WorkloadActions connectionId="c1" namespace="team-a" d={details} />);
    await waitFor(() => expect(screen.getByRole("button", { name: "Масштабировать" })).toBeDisabled());
    expect(screen.getByRole("button", { name: "Масштабировать" })).toHaveAttribute("title", "Нет прав: patch deployments/scale.apps");
    expect(screen.getByRole("button", { name: "Границы HPA" })).toBeEnabled();
    unmount();
    vi.mocked(api.listConnections).mockResolvedValue([{ ...dev, readOnly: true }]);
    renderWithClient(<WorkloadActions connectionId="c1" namespace="team-a" d={details} />);
    await waitFor(() => expect(screen.getByRole("button", { name: "Перезапустить" })).toBeDisabled());
  });
});
