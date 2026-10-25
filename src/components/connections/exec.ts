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

/**
 * Как `auth::exec::is_secret_name` в Rust: значения таких переменных и ключей не показываются.
 * `PASS` покрывает PASSWORD, PASSWD и PASSPHRASE.
 */
export function isSecretName(name: string): boolean {
  const n = name.toUpperCase();
  return ["TOKEN", "SECRET", "PASS", "KEY", "BEARER", "COOKIE", "CREDENTIAL"].some((w) => n.includes(w));
}

function quote(s: string): string {
  return s === "" || /[\s'"\\$`]/.test(s) ? `'${s.replace(/'/g, `'\\''`)}'` : s;
}

/**
 * Аргументы для строки запуска, значения секретных ключей скрыты, как в Rust (`secret_args`).
 * Ключ — аргумент с `-`, чьё имя без ведущих дефисов и до первого `=` подходит под `isSecretName`.
 * `--имя=значение` показывается как `--имя=••••`; в паре `--имя значение` скрывается следующий
 * элемент, каким бы он ни был; ключ без значения в конце списка ничего не скрывает. Пустое
 * значение не скрывается.
 */
function maskedArgs(args: string[]): string[] {
  const out: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    const eq = a.indexOf("=");
    const secret = a.startsWith("-") && isSecretName((eq < 0 ? a : a.slice(0, eq)).replace(/^-+/, ""));
    if (!secret) {
      out.push(quote(a));
    } else if (eq >= 0) {
      // Имя с пробелом (`--client secret=x`) квотируется вместе с маской, как и открытый аргумент.
      out.push(a.length > eq + 1 ? quote(`${a.slice(0, eq + 1)}••••`) : quote(a));
    } else if (i + 1 < args.length) {
      const value = args[++i];
      out.push(quote(a), value ? "••••" : quote(value));
    } else {
      out.push(quote(a));
    }
  }
  return out;
}

/** Строка запуска для подтверждения: переменные и значения секретных ключей скрыты, путь к команде и аргументы. */
export function launchLine(e: ExecAuth): string {
  const env = e.env.map((v) => `${v.name}=${isSecretName(v.name) && v.value ? "••••" : quote(v.value)}`);
  return [...env, quote(e.command), ...maskedArgs(e.args)].join(" ");
}

/**
 * Ключ строки запуска: те же поля, что в отпечатке Rust (`fingerprint`). Подтверждение
 * действует, пока ключ строки совпадает с ключом подтверждённой.
 */
export function launchKey(e: ExecAuth): string {
  return JSON.stringify([e.command, e.args, e.env.map((v) => [v.name, v.value]), e.apiVersion, e.provideClusterInfo]);
}

function sameLaunch(a: ExecAuth, b: ExecAuth): boolean {
  return launchKey(a) === launchKey(b);
}

/**
 * Нужно ли подтвердить строку запуска: новый exec-плагин, строка изменилась или сохранённая
 * не подтверждена. Окончательно решает Rust по отпечатку.
 */
export function needsExecConfirmation(initial: AuthConfig, current: AuthConfig): boolean {
  if (current.method !== "exec") return false;
  return !(initial.method === "exec" && initial.approved && sameLaunch(initial, current));
}
