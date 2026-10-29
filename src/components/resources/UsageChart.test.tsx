import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

const plots: { opts: { series: { label?: string }[] }; destroy: () => void }[] = [];
vi.mock("uplot", () => ({
  default: vi.fn().mockImplementation(function (this: object, opts: { series: { label?: string }[] }) {
    const p = { opts, destroy: vi.fn(), setSize: vi.fn() };
    plots.push(p);
    return p;
  }),
}));
globalThis.ResizeObserver ??= class {
  observe() {}
  disconnect() {}
  unobserve() {}
} as unknown as typeof ResizeObserver;

import { UsageChart } from "./UsageChart";

describe("UsageChart", () => {
  it("ряды контейнеров, линии request и limit, отметки; при размонтировании график уничтожается", () => {
    const { unmount } = render(
      <UsageChart
        title="CPU"
        series={[{ container: "app", points: [[1, 0.1]] }]}
        lines={[
          { label: "app request", value: 0.2, dash: "request" },
          { label: "app limit", value: 0.5, dash: "limit" },
        ]}
        markers={[]}
        markerLabel="Рестарты и OOM"
        format={String}
      />,
    );
    expect(plots[0].opts.series.map((s) => s.label)).toEqual([undefined, "app", "app request", "app limit", "Рестарты и OOM"]);
    unmount();
    expect(plots[0].destroy).toHaveBeenCalled();
  });
});
