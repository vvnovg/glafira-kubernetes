import type { AuthConfig, ExecAuth, ExecEnv } from "../../api/types";

/** Exec-плагин нового подключения: версия API — `v1`, как у современных плагинов. */
export function newExec(): ExecAuth {
  return { command: "", args: [], env: [], apiVersion: "client.authentication.k8s.io/v1", provideClusterInfo: false, approved: null };
}

/** Аргументы — по одному в строке; пустые строки пропускаются. */
export function parseArgs(text: string): string[] {
  return text
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function formatArgs(args: string[]): string {
  return args.join("\n");
}

/** Переменные — `ИМЯ=значение` по одной в строке; значение может содержать `=`. */
export function parseEnv(text: string): ExecEnv[] {
  return text
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((line) => {
      const i = line.indexOf("=");
      return i < 0 ? { name: line, value: "" } : { name: line.slice(0, i).trim(), value: line.slice(i + 1) };
    });
}

export function formatEnv(env: ExecEnv[]): string {
  return env.map((v) => `${v.name}=${v.value}`).join("\n");
}

/** Как `auth::exec::is_secret_name` в Rust: значения таких переменных не показываются. */
export function isSecretName(name: string): boolean {
  const n = name.toUpperCase();
  return ["TOKEN", "SECRET", "PASSWORD", "PASSWD", "KEY", "CREDENTIAL"].some((w) => n.includes(w));
}

function quote(s: string): string {
  return s === "" || /[\s'"\\$`]/.test(s) ? `'${s.replace(/'/g, `'\\''`)}'` : s;
}

/** Строка запуска для подтверждения: переменные (секреты скрыты), путь к команде и аргументы. */
export function launchLine(e: ExecAuth): string {
  const env = e.env.map((v) => `${v.name}=${isSecretName(v.name) && v.value ? "••••" : quote(v.value)}`);
  return [...env, quote(e.command), ...e.args.map(quote)].join(" ");
}

function sameLaunch(a: ExecAuth, b: ExecAuth): boolean {
  const key = (e: ExecAuth) => JSON.stringify([e.command, e.args, e.env.map((v) => [v.name, v.value]), e.apiVersion, e.provideClusterInfo]);
  return key(a) === key(b);
}

/**
 * Нужно ли подтвердить строку запуска: новый exec-плагин, строка изменилась или сохранённая
 * не подтверждена. Окончательно решает Rust по отпечатку.
 */
export function needsExecConfirmation(initial: AuthConfig, current: AuthConfig): boolean {
  if (current.method !== "exec") return false;
  return !(initial.method === "exec" && initial.approved && sameLaunch(initial, current));
}
