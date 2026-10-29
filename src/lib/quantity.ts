/** Количества Kubernetes в интерфейсе (спец. §7): CPU приходит в ядрах, память — в байтах. */

const MI = 1024 * 1024;
const GI = 1024 * MI;

/** CPU строкой quantity, как `format_cpu` в Rust: целые ядра — «2», иначе милли-ядра — «250m». */
export function cpuQuantity(cores: number): string {
  const milli = Math.round(cores * 1000);
  return milli % 1000 === 0 ? String(milli / 1000) : `${milli}m`;
}

/** Память строкой quantity, как `format_memory` в Rust: целые гибибайты — «2Gi», иначе мебибайты — «512Mi». */
export function memoryQuantity(bytes: number): string {
  const b = Math.round(bytes);
  return b !== 0 && b % GI === 0 ? `${b / GI}Gi` : `${Math.round(b / MI)}Mi`;
}

/** CPU для таблиц; нет значения — «—». */
export const cpuText = (cores: number | null | undefined) => (cores == null ? "—" : cpuQuantity(cores));

/** Память для таблиц; нет значения — «—». */
export const memoryText = (bytes: number | null | undefined) => (bytes == null ? "—" : memoryQuantity(bytes));

/** Разница со знаком: «+250m», «−512Mi», «0». */
export function signedText(value: number | null | undefined, text: (v: number) => string): string {
  if (value == null) return "—";
  const t = text(Math.abs(value));
  if (t === "0" || t === "0Mi" || t === "0m") return "0";
  return `${value < 0 ? "−" : "+"}${t}`;
}

/** Доля 0–1 процентами: «40 %». */
export const percentText = (share: number | null | undefined) => (share == null ? "—" : `${Math.round(share * 100)} %`);
