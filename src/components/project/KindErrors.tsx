import type { KindError } from "../../api/types";
import { tr } from "../../i18n/text";

/**
 * Виды workloads, которые не прочитались (нет прав, сбой): `Вид: — причина`. `listWorkloads` не падает целиком —
 * остальные виды приходят, а эти приходят причинами, и экран обязан их показать (спец. §15).
 */
export function KindErrors({ errors }: { errors: KindError[] }) {
  return (
    <>
      {errors.map((e) => (
        <p key={e.kind} className="muted">
          {e.kind}: — {tr(e.error)}
        </p>
      ))}
    </>
  );
}
