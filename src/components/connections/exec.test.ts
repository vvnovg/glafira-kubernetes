import { describe, expect, it } from "vitest";
import type { AuthConfig } from "../../api/types";
import { formatArgs, formatEnv, launchLine, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";

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

  it("подтверждение нужно для новой, изменённой и неподтверждённой строки", () => {
    const approved = exec({ approved: "fp" });
    expect(needsExecConfirmation({ method: "token" }, exec())).toBe(true);
    expect(needsExecConfirmation(exec(), exec())).toBe(true);
    expect(needsExecConfirmation(approved, exec())).toBe(false);
    expect(needsExecConfirmation(approved, exec({ args: ["get-token"] }))).toBe(true);
    expect(needsExecConfirmation(approved, { method: "token" })).toBe(false);
  });
});
