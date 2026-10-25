import { describe, expect, it } from "vitest";
import type { AuthConfig } from "../../api/types";
import { formatArgs, formatEnv, isSecretName, launchKey, launchLine, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";

const exec = (patch: Partial<ReturnType<typeof newExec>> = {}): AuthConfig => ({
  method: "exec",
  ...newExec(),
  command: "/opt/homebrew/bin/kubelogin",
  args: ["get-token", "--oidc-issuer-url=https://sso.corp"],
  ...patch,
});

describe("exec", () => {
  it("аргументы и переменные — по строкам", () => {
    expect(parseArgs(" get-token \n\n--login=azurecli\n")).toEqual(["get-token", "--login=azurecli"]);
    expect(formatArgs(["a", "b"])).toBe("a\nb");
    expect(parseEnv("AWS_PROFILE=dev\nX=a=b\n\nEMPTY")).toEqual([
      { name: "AWS_PROFILE", value: "dev" },
      { name: "X", value: "a=b" },
      { name: "EMPTY", value: "" },
    ]);
    expect(formatEnv([{ name: "A", value: "1" }])).toBe("A=1");
  });

  it("строка запуска: секреты скрыты, аргументы с пробелами в кавычках", () => {
    const e = {
      ...newExec(),
      command: "/usr/local/bin/aws",
      args: ["eks", "get-token", "--cluster-name", "my cluster"],
      env: [
        { name: "AWS_PROFILE", value: "dev" },
        { name: "AWS_SECRET_ACCESS_KEY", value: "s3cr3t" },
      ],
    };
    expect(launchLine(e)).toBe("AWS_PROFILE=dev AWS_SECRET_ACCESS_KEY=•••• /usr/local/bin/aws eks get-token --cluster-name 'my cluster'");
  });

  it("секретные имена — как в Rust: PASS, BEARER и COOKIE тоже", () => {
    for (const name of ["GIT_PASSPHRASE", "AUTH_BEARER", "SESSION_COOKIE", "password", "client_secret"]) {
      expect(isSecretName(name), name).toBe(true);
    }
    for (const name of ["AWS_PROFILE", "KUBECONFIG"]) {
      expect(isSecretName(name), name).toBe(false);
    }
  });

  it("строка запуска: значения секретных ключей скрыты", () => {
    const line = (args: string[]) => launchLine({ ...newExec(), command: "plugin", args });
    expect(line(["get-token", "--oidc-client-secret=abc"])).toBe("plugin get-token --oidc-client-secret=••••");
    expect(line(["--client-secret", "abc", "--login=azurecli"])).toBe("plugin --client-secret •••• --login=azurecli");
    expect(line(["-token=x"])).toBe("plugin -token=••••");
    expect(line(["--client-secret", "--next"])).toBe("plugin --client-secret ••••");
    expect(line(["--cluster-name", "my cluster"])).toBe("plugin --cluster-name 'my cluster'");
    // Имя с пробелом квотируется и в замаскированном `--имя=значение`, как в открытом аргументе.
    expect(line(["--client secret=abc"])).toBe("plugin '--client secret=••••'");
  });

  it("ключ строки запуска меняется с командой, аргументами, переменными и версией, но не с отпечатком", () => {
    const key = (patch: Partial<ReturnType<typeof newExec>> = {}) => launchKey({ ...newExec(), command: "plugin", ...patch });
    expect(key({ approved: "fp" })).toBe(key());
    for (const patch of [
      { command: "other" },
      { args: ["get-token"] },
      { env: [{ name: "A", value: "1" }] },
      { apiVersion: "client.authentication.k8s.io/v1beta1" },
      { provideClusterInfo: true },
    ]) {
      expect(key(patch), JSON.stringify(patch)).not.toBe(key());
    }
  });

  it("строка запуска: ключ без значения и пустое значение ничего не скрывают", () => {
    const line = (args: string[]) => launchLine({ ...newExec(), command: "plugin", args });
    expect(line(["get-token", "--password"])).toBe("plugin get-token --password");
    expect(line(["--password="])).toBe("plugin --password=");
    expect(line(["--password", ""])).toBe("plugin --password ''");
  });

  it("подтверждение нужно для новой, изменённой и неподтверждённой строки", () => {
    const approved = exec({ approved: "fp" });
    expect(needsExecConfirmation({ method: "token" }, exec())).toBe(true);
    expect(needsExecConfirmation(exec(), exec())).toBe(true);
    expect(needsExecConfirmation(approved, exec())).toBe(false);
    expect(needsExecConfirmation(approved, exec({ args: ["get-token"] }))).toBe(true);
    expect(needsExecConfirmation(approved, { method: "token" })).toBe(false);
  });
});
