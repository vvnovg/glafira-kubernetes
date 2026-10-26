import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { TruncatedNote } from "./TruncatedNote";

const TEXT = "Список неполный: показаны первые 10 000 объектов";

describe("TruncatedNote", () => {
  it("список обрезан — предупреждение о первых 10 000", () => {
    render(<TruncatedNote truncated />);
    expect(screen.getByText(TEXT)).toHaveClass("warn");
    expect(screen.getByRole("status")).toHaveTextContent(TEXT);
  });

  it("список целиком или ещё не прочитан — ничего", () => {
    const { container, rerender } = render(<TruncatedNote truncated={false} />);
    expect(container).toBeEmptyDOMElement();
    rerender(<TruncatedNote truncated={undefined} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("класс можно заменить: в дереве проектов заметка мелкая", () => {
    render(<TruncatedNote truncated className="tree-notice" />);
    expect(screen.getByText(TEXT)).toHaveClass("tree-notice");
    expect(screen.getByText(TEXT)).not.toHaveClass("warn");
  });
});
