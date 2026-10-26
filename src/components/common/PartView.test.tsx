import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it } from "vitest";
import { renderWithClient } from "../../test/render";
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

beforeEach(() => useWorkspace.setState({ tabs: [], activeId: null }));

describe("PartView и ReadError", () => {
  it("часть экрана: данные, «—» с причиной без прав, баннер при ошибке", () => {
    const { rerender } = renderWithClient(<PartView part={{ data: 3 }}>{(n) => <span>всего {n}</span>}</PartView>);
    expect(screen.getByText("всего 3")).toBeInTheDocument();
    rerender(<PartView part={{ error: FORBIDDEN }}>{() => <span>x</span>}</PartView>);
    expect(screen.getByText("— Нет прав: list secrets в проекте «team-a»")).toBeInTheDocument();
    rerender(<PartView part={{ error: { code: "TIMEOUT", key: "errors.k8s.timeout", message: "t", retryable: true } }}>{() => <span>x</span>}</PartView>);
    expect(screen.getByRole("alert")).toHaveTextContent("Сервер не ответил вовремя");
  });

  it("истёкшая сессия ведёт на вкладку подключения, удалённый объект — пояснение", async () => {
    const user = userEvent.setup();
    const { rerender } = renderWithClient(
      <ReadError error={{ code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "x", retryable: false }} connectionId="c1" />,
    );
    await user.click(screen.getByRole("button", { name: "Войти снова" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "cluster", connectionId: "c1" });
    rerender(<ReadError error={{ code: "NOT_FOUND", key: "errors.k8s.notFound", message: "x", retryable: false }} connectionId="c1" />);
    expect(screen.getByText("Объект удалён или переименован — закройте вкладку")).toBeInTheDocument();
    rerender(<ReadError error={FORBIDDEN} connectionId="c1" onRetry={() => undefined} />);
    expect(screen.queryByRole("button", { name: "Повторить" })).not.toBeInTheDocument();
  });
});
