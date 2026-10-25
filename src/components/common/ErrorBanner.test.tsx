import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ErrorBanner } from "./ErrorBanner";

describe("ErrorBanner", () => {
  it("переводит ошибку по ключу словаря", () => {
    render(
      <ErrorBanner
        error={{
          code: "FORBIDDEN",
          key: "errors.k8s.forbiddenIn",
          params: { verb: "list", resource: "pods", namespace: "team-a" },
          message: "Not allowed: list pods in project “team-a”",
          retryable: false,
        }}
      />,
    );
    expect(screen.getByText("Нет прав: list pods в проекте «team-a»")).toBeInTheDocument();
  });

  it("errors.generic показывает исходный текст как есть", () => {
    render(
      <ErrorBanner
        error={{ code: "INVALID_INPUT", key: "errors.generic", params: { message: "плохо & <b>" }, message: "плохо & <b>", retryable: false }}
      />,
    );
    expect(screen.getByText("плохо & <b>")).toBeInTheDocument();
  });

  it("неизвестный ключ — английский текст; кнопки и подробности переведены", () => {
    const onRetry = vi.fn();
    render(
      <ErrorBanner
        error={{ code: "TIMEOUT", key: "errors.nope", message: "Timed out", details: "trace", retryable: true }}
        onRetry={onRetry}
      />,
    );
    expect(screen.getByText("Timed out")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Повторить" })).toBeInTheDocument();
    expect(screen.getByText("Подробности")).toBeInTheDocument();
  });

  it("ошибка без ключа (старый формат) — message", () => {
    render(<ErrorBanner error={{ code: "CONFLICT", message: "Объект изменён", retryable: false }} />);
    expect(screen.getByText("Объект изменён")).toBeInTheDocument();
    expect(screen.getByText("(CONFLICT)")).toBeInTheDocument();
  });
});
