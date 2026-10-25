#!/usr/bin/env node
// Добавляет строки в словари: JSON вида {"en": {...}, "ru": {...}} из stdin сливается вглубь с
// src/locales/<код>.json. Ключ с другим значением — ошибка (защита от случайной перезаписи);
// заменить значение — флаг --replace. Только модули node:.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

/** Сливает `add` в `base`; возвращает список конфликтов «путь: было → стало». */
export function deepMerge(base, add, replace = false, prefix = "", conflicts = []) {
  for (const [k, v] of Object.entries(add)) {
    const path = prefix ? `${prefix}.${k}` : k;
    if (v && typeof v === "object") {
      if (base[k] !== undefined && (typeof base[k] !== "object" || base[k] === null)) conflicts.push(`${path}: строка → объект`);
      else deepMerge((base[k] ??= {}), v, replace, path, conflicts);
    } else if (base[k] !== undefined && base[k] !== v && !replace) conflicts.push(`${path}: «${base[k]}» → «${v}»`);
    else base[k] = v;
  }
  return conflicts;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const replace = process.argv.includes("--replace");
  const input = JSON.parse(readFileSync(0, "utf8"));
  const problems = [];
  const merged = {};
  for (const [code, add] of Object.entries(input)) {
    const file = join(process.cwd(), "src/locales", `${code}.json`);
    const dict = existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : {};
    problems.push(...deepMerge(dict, add, replace).map((c) => `${code}: ${c}`));
    merged[file] = dict;
  }
  if (problems.length) {
    console.error(problems.map((p) => `✗ ${p}`).join("\n"));
    process.exit(1);
  }
  mkdirSync(join(process.cwd(), "src/locales"), { recursive: true });
  for (const [file, dict] of Object.entries(merged)) writeFileSync(file, `${JSON.stringify(dict, null, 2)}\n`);
  console.log(`✓ словари обновлены: ${Object.keys(input).join(", ")}`);
}
