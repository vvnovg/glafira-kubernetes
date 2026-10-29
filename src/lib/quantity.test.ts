import { describe, expect, it } from "vitest";
import { cpuQuantity, cpuText, memoryQuantity, memoryText, percentText, signedText } from "./quantity";

const MI = 1024 * 1024;

describe("quantity", () => {
  it("CPU и память — как quantity из Rust", () => {
    expect([cpuQuantity(0.25), cpuQuantity(2), cpuQuantity(0.0123), cpuQuantity(1.5)]).toEqual(["250m", "2", "12m", "1500m"]);
    expect([memoryQuantity(512 * MI), memoryQuantity(2048 * MI), memoryQuantity(30.4 * MI)]).toEqual(["512Mi", "2Gi", "30Mi"]);
    expect([cpuText(null), memoryText(undefined)]).toEqual(["—", "—"]);
  });

  it("разница со знаком и доли", () => {
    expect([signedText(-1.2, cpuQuantity), signedText(512 * MI, memoryQuantity), signedText(0.0001, cpuQuantity)]).toEqual(["−1200m", "+512Mi", "0"]);
    expect([percentText(0.404), percentText(null)]).toEqual(["40 %", "—"]);
  });
});
