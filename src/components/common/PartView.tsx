import type { ReactNode } from "react";
import type { AppError, Part } from "../../api/types";
import { tr } from "../../i18n/text";
import { ErrorBanner } from "./ErrorBanner";

/** Нет прав — «—» с причиной, иная ошибка — баннер (спец. §15, «Частичные результаты»). */
export function Unavailable({ error }: { error: AppError }) {
  if (error.code === "FORBIDDEN") return <p className="muted">— {tr(error)}</p>;
  return <ErrorBanner error={error} />;
}

/** Часть экрана: данные или причина, почему их нет; остальной экран показывается. */
export function PartView<T>({ part, children }: { part: Part<T> | null | undefined; children: (data: T) => ReactNode }) {
  if (!part) return null;
  if (part.error) return <Unavailable error={part.error} />;
  return part.data === undefined ? null : <>{children(part.data)}</>;
}
