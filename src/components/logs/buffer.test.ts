import { describe, expect, it } from "vitest";
import type { LogLine } from "../../api/types";
import { appendLines } from "./buffer";

const line = (text: string): LogLine => ({ ts: null, text, truncated: false });

describe("буфер логов", () => {
  it("держит последние строки", () => {
    const b = appendLines([line("1"), line("2")], [line("3"), line("4")], 3);
    expect(b.map((l) => l.text)).toEqual(["2", "3", "4"]);
    expect(appendLines([], [line("a")], 3)).toHaveLength(1);
  });
});
