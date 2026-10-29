import { useCallback, useEffect, useState } from "react";
import type { ProposalPrefill } from "../../state/workspace";

/**
 * Активное предзаполнение формы из предложения LLM. Новый nonce (повторное «Открыть в форме») включает его заново;
 * закрытие или успех формы снимает, чтобы форма, открытая вручную позже, была пустой и без proposalId.
 * Ключ — nonce, а не идентичность объекта: контейнеры могут пересобирать объект на каждом рендере.
 */
export function useActivePrefill(prefill: ProposalPrefill | undefined): [ProposalPrefill | undefined, () => void] {
  const [active, setActive] = useState(prefill);
  const nonce = prefill?.nonce;
  useEffect(() => {
    if (nonce !== undefined) setActive(prefill);
    // Зависимость только от nonce — намеренно (см. комментарий выше).
  }, [nonce]);
  const clear = useCallback(() => setActive(undefined), []);
  return [active, clear];
}
