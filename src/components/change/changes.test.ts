import { describe, expect, it } from "vitest";
import type { Change, NamespaceAccess, Verbs } from "../../api/types";
import { i18n } from "../../i18n";
import { allowed, changeRight, hasValues, warningText, withValues } from "./changes";

const t = i18n.t.bind(i18n);
const web = { kind: "Deployment", name: "web" } as const;
const verbs = (patch: boolean): Verbs => ({ get: true, list: true, watch: true, patch, delete: false, create: false });

describe("изменения", () => {
  it("права: масштаб — подресурс scale, перезапуск DeploymentConfig — instantiate", () => {
    expect(changeRight({ kind: "scale", namespace: "a", workload: web, replicas: 2 })).toEqual({ resource: "deployments/scale.apps", verb: "patch" });
    expect(changeRight({ kind: "rolloutRestart", namespace: "a", workload: { kind: "DeploymentConfig", name: "x" } })).toEqual({
      resource: "deploymentconfigs/instantiate.apps.openshift.io",
      verb: "create",
    });
    const access: NamespaceAccess = { resources: { "deployments.apps": verbs(false) }, incomplete: false };
    const set: Change = { kind: "setResources", namespace: "a", workload: web, containers: [] };
    expect(allowed(access, set)).toBe(false);
    expect(allowed({ ...access, incomplete: true }, set)).toBe(true);
    expect(allowed(access, { kind: "deletePod", namespace: "a", pod: "p" })).toBe(true);
  });

  it("пустые строки формы ресурсов не отправляются", () => {
    const c: Change = {
      kind: "setResources",
      namespace: "a",
      workload: web,
      containers: [
        { name: "app", requests: { cpu: "250m" }, limits: {} },
        { name: "proxy", requests: {}, limits: {} },
        { name: "init", requests: {}, limits: { memory: null } },
      ],
    };
    expect(withValues(c)).toMatchObject({ containers: [{ name: "app" }, { name: "init" }] });
    expect(hasValues({ ...c, containers: [{ name: "proxy", requests: {}, limits: {} }] })).toBe(false);
  });

  it("предупреждения словами", () => {
    expect(warningText({ code: "quota", quota: "compute", resource: "requests.cpu", needed: 1.5, free: 0.5 }, t)).toBe(
      "Квоте compute не хватит requests.cpu: нужно 1500m, свободно 500m — поды новой ревизии не создадутся, выкладка встанет",
    );
    expect(warningText({ code: "unverified", what: "quota" }, t)).toBe("Не удалось прочитать квоты проекта: изменение не проверено по этим данным");
    expect(warningText({ code: "limitRange", container: "app", field: "memoryLimit", bound: "max", limit: 1024 ** 3 }, t)).toBe(
      "app: memory limit больше максимума LimitRange 1Gi — сервер отклонит изменение",
    );
  });
});
