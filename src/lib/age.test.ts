import { describe, expect, it } from "vitest";
import { formatAge, formatDateTime } from "./age";

describe("age", () => {
  it("возраст коротко: секунды, минуты, часы, дни", () => {
    const now = 1_000_000_000_000;
    const ago = (s: number) => formatAge(now / 1000 - s, now);
    expect(ago(45)).toBe("45 с");
    expect(ago(150)).toBe("2 мин");
    expect(ago(3 * 3600)).toBe("3 ч");
    expect(ago(5 * 86_400)).toBe("5 д");
    expect(formatAge(now / 1000 + 30, now)).toBe("0 с");
    expect(formatAge(null)).toBe("—");
  });

  it("дата и время", () => {
    expect(formatDateTime(undefined)).toBe("—");
    expect(formatDateTime(0)).toContain("1970");
  });
});
