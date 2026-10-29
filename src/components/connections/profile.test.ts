import { describe, expect, it } from "vitest";
import { formatNamespaces, newProfile, parseNamespaces, withAuthMethod, withCaMode, withMetricsMode, withProxyMode } from "./profile";

describe("profile", () => {
  it("новый профиль: вход через браузер, системный CA и прокси, dev", () => {
    const p = newProfile();
    expect(p.auth).toEqual({ method: "browser" });
    expect(p.ca).toEqual({ mode: "system" });
    expect(p.proxy).toEqual({ mode: "system" });
    expect(p.environment).toBe("dev");
    expect([p.requestTimeoutMs, p.connectTimeoutMs]).toEqual([30000, 10000]);
  });

  it("переключение способа входа, CA и прокси сохраняет введённое", () => {
    const cert = withAuthMethod({ method: "token" }, "clientCert", "PEM");
    expect(cert).toEqual({ method: "clientCert", certificatePem: "PEM" });
    expect(withAuthMethod(cert, "clientCert")).toEqual(cert);
    expect(withAuthMethod(cert, "token")).toEqual({ method: "token" });
    const exec = withAuthMethod(cert, "exec");
    expect(exec).toMatchObject({ method: "exec", command: "", args: [], apiVersion: "client.authentication.k8s.io/v1" });
    const last = { command: "/usr/local/bin/oc", args: ["get-token"], env: [], apiVersion: "v1", provideClusterInfo: false };
    expect(withAuthMethod({ method: "token" }, "exec", "", last)).toEqual({ method: "exec", ...last });
    expect(withCaMode({ mode: "system" }, "pem", "CA")).toEqual({ mode: "pem", pem: "CA" });
    expect(withCaMode({ mode: "pem", pem: "X" }, "insecure")).toEqual({ mode: "insecure" });
    expect(withProxyMode({ mode: "system" }, "url", "http://p:1")).toEqual({ mode: "url", url: "http://p:1" });
    expect(withProxyMode({ mode: "url", url: "u" }, "none")).toEqual({ mode: "none" });
  });

  it("проекты через запятую, пробелы и переводы строк", () => {
    expect(parseNamespaces(" team-a, team-b\nteam-a  ops ,")).toEqual(["team-a", "team-b", "ops"]);
    expect(parseNamespaces("")).toEqual([]);
    expect(formatNamespaces(["a", "b"])).toBe("a, b");
  });
});

describe("withMetricsMode", () => {
  it("свой адрес не теряется при переключении", () => {
    const thanos = withMetricsMode({ mode: "auto" }, "thanos");
    expect(thanos).toEqual({ mode: "thanos", url: "", auth: "connection", ca: { mode: "connection" } });
    const own = { mode: "thanos" as const, url: "https://t.example.com", auth: "bearer" as const, ca: { mode: "connection" as const } };
    expect(withMetricsMode(own, "prometheus")).toEqual({ ...own, mode: "prometheus" });
    expect(withMetricsMode(own, "auto")).toEqual({ mode: "auto" });
    expect(withMetricsMode({ mode: "auto" }, "prometheus", own)).toEqual({ ...own, mode: "prometheus" });
  });
});
