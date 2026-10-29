import { describe, expect, it } from "vitest";
import { chartData } from "./chart";

describe("chartData", () => {
  it("общая ось времени, пропуски — null, линии и отметки", () => {
    const data = chartData(
      [
        { container: "app", points: [[100, 0.2], [400, 0.3]] },
        { container: "proxy", points: [[100, 0.05], [700, 0.04]] },
      ],
      [{ label: "app request", value: 0.25, dash: "request" }],
      [{ container: "app", time: 400, count: 1 }],
    );
    expect(data).toEqual([
      [100, 400, 700],
      [0.2, 0.3, null],
      [0.05, null, 0.04],
      [0.25, 0.25, 0.25],
      [null, 0.3, null],
    ]);
  });
});
