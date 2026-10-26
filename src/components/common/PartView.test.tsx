import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { connect: vi.fn() } };
});

import { api } from "../../api/client";
import type { AppError } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { PartView } from "./PartView";
import { ReadError } from "./ReadError";

const FORBIDDEN: AppError = {
  code: "FORBIDDEN",
  key: "errors.k8s.forbiddenIn",
  params: { verb: "list", resource: "secrets", namespace: "team-a" },
  message: "Not allowed",
  retryable: false,
};

const SESSION_EXPIRED: AppError = { code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "x", retryable: false };

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("PartView и ReadError", () => {
  it("часть экрана: данные, «—» с причиной без прав, баннер при ошибке", () => {
    const { rerender } = renderWithClient(<PartView part={{ data: 3 }}>{(n) => <span>всего {n}</span>}</PartView>);
    expect(screen.getByText("всего 3")).toBeInTheDocument();
    rerender(<PartView part={{ error: FORBIDDEN }}>{() => <span>x</span>}</PartView>);
    expect(screen.getByText("— Нет прав: list secrets в проекте «team-a»")).toBeInTheDocument();
    rerender(<PartView part={{ error: { code: "TIMEOUT", key: "errors.k8s.timeout", message: "t", retryable: true } }}>{() => <span>x</span>}</PartView>);
    expect(screen.getByRole("alert")).toHaveTextContent("Сервер не ответил вовремя");
  });

  it("истёкшая сессия: «Войти снова» подключается заново и открывает вкладку подключения, где ждёт ошибка входа", async () => {
    vi.mocked(api.connect).mockRejectedValue(SESSION_EXPIRED);
    const user = userEvent.setup();
    const { client } = renderWithClient(<ReadError error={SESSION_EXPIRED} connectionId="c1" />);
    await user.click(screen.getByRole("button", { name: "Войти снова" }));
    await waitFor(() => expect(api.connect).toHaveBeenCalledWith("c1", true));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "cluster", connectionId: "c1" });
    // Ошибка подключения — в запросе вкладки подключения: там видны способы войти.
    await waitFor(() => expect(client.getQueryState(["connection", "c1"])?.error).toEqual(SESSION_EXPIRED));
  });

  it("пока идёт подключение, «Войти снова» недоступна; объект удалён — пояснение; нет прав — без «Повторить»", async () => {
    vi.mocked(api.connect).mockReturnValue(new Promise(() => undefined));
    const user = userEvent.setup();
    const signIn = renderWithClient(<ReadError error={SESSION_EXPIRED} connectionId="c1" />);
    await user.click(screen.getByRole("button", { name: "Войти снова" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Войти снова" })).toBeDisabled());
    expect(api.connect).toHaveBeenCalledTimes(1);
    signIn.unmount();
    const gone = renderWithClient(<ReadError error={{ code: "NOT_FOUND", key: "errors.k8s.notFound", message: "x", retryable: false }} connectionId="c1" />);
    expect(screen.getByText("Объект удалён или переименован — закройте вкладку")).toBeInTheDocument();
    gone.unmount();
    renderWithClient(<ReadError error={FORBIDDEN} connectionId="c1" onRetry={() => undefined} />);
    expect(screen.queryByRole("button", { name: "Повторить" })).not.toBeInTheDocument();
  });
});
