import { describe, expect, it } from "vitest";
import type { NamespaceAccess, Verbs } from "../../api/types";
import { WORKLOAD_RESOURCES, canList } from "./access";

const verbs = (list: boolean): Verbs => ({ get: list, list, watch: list });
const access = (resources: Record<string, Verbs>, incomplete = false): NamespaceAccess => ({ resources, incomplete });

describe("canList", () => {
  it("права ещё неизвестны — показывается всё", () => {
    expect(canList(undefined, "pods")).toBe(true);
    expect(canList(undefined, ...WORKLOAD_RESOURCES)).toBe(true);
  });

  it("сервер перечислил права не целиком (incomplete) — ничего не скрывается, даже явный запрет", () => {
    expect(canList(access({ pods: verbs(false) }, true), "pods")).toBe(true);
    expect(canList(access({}, true), "events")).toBe(true);
  });

  it("явный запрет скрывает, явное право показывает", () => {
    const a = access({ pods: verbs(true), events: verbs(false) });
    expect(canList(a, "pods")).toBe(true);
    expect(canList(a, "events")).toBe(false);
  });

  it("ресурса нет в ответе — считается разрешённым, решает API-сервер", () => {
    expect(canList(access({ pods: verbs(true) }), "configmaps")).toBe(true);
    expect(canList(access({}), "events")).toBe(true);
  });

  it("несколько ресурсов подвкладки: достаточно одного читаемого", () => {
    const a = access({ configmaps: verbs(false), secrets: verbs(true) });
    expect(canList(a, "configmaps", "secrets")).toBe(true);
    expect(canList(access({ configmaps: verbs(false), secrets: verbs(false) }), "configmaps", "secrets")).toBe(false);
  });

  it("Workloads скрыты, только когда нельзя читать все шесть видов", () => {
    const denied = Object.fromEntries(WORKLOAD_RESOURCES.map((k) => [k, verbs(false)]));
    expect(WORKLOAD_RESOURCES).toHaveLength(6);
    expect(canList(access(denied), ...WORKLOAD_RESOURCES)).toBe(false);
    // Достаточно любого одного вида.
    for (const readable of WORKLOAD_RESOURCES) {
      expect(canList(access({ ...denied, [readable]: verbs(true) }), ...WORKLOAD_RESOURCES)).toBe(true);
    }
    // Вид, которого нет в ответе (например, DeploymentConfig вне OpenShift), считается разрешённым.
    const withoutDc = Object.fromEntries(WORKLOAD_RESOURCES.filter((k) => k !== "deploymentconfigs.apps.openshift.io").map((k) => [k, verbs(false)]));
    expect(canList(access(withoutDc), ...WORKLOAD_RESOURCES)).toBe(true);
  });
});
