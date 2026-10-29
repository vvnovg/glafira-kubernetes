import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { listen } from "@tauri-apps/api/event";
import { useEffect } from "react";
import { api } from "../api/client";
import type { AnnotationStatus, HealthView } from "../api/types";

/** Статус, который человек ставит предложению: «Отклонить» или «Открыть снова». */
type ProposalAction = "rejected" | "open";

/** Событие из Rust (`crate::health::CHANGED_EVENT`): изменилась проверка или замечания подключения. */
export const HEALTH_EVENT = "health://changed";
export const healthKey = (connectionId: string) => ["health", connectionId] as const;
/** Ключ мутации «Проверить кластер»: по нему все CheckBar подключения видят идущую проверку (`useIsMutating`). */
export const healthCheckKey = (connectionId: string) => ["healthCheck", connectionId] as const;

/**
 * Данные свежи минуту: значок стоит в каждой строке списка и появляется при прокрутке заново, а `get_health` —
 * чтение файла и разбор JSON. Устаревания можно не бояться: любое изменение приходит событием `health://changed`
 * (оно помечает запрос устаревшим), а действия кладут свежее состояние через `setQueryData`.
 */
const HEALTH_STALE_MS = 60_000;

/** Последняя проверка, скрытые находки и замечания LLM подключения. */
export function useHealth(connectionId: string) {
  return useQuery({ queryKey: healthKey(connectionId), queryFn: () => api.getHealth(connectionId), staleTime: HEALTH_STALE_MS });
}

/** Подписка на изменения — одна на приложение (в Layout). Вне Tauri (тесты) молча не работает. */
export function useHealthEvents(): void {
  const qc = useQueryClient();
  useEffect(() => {
    let stop: (() => void) | undefined;
    let unmounted = false;
    listen<{ connectionId: string }>(HEALTH_EVENT, (e) => {
      void qc.invalidateQueries({ queryKey: healthKey(e.payload.connectionId) });
    })
      .then((unlisten) => (unmounted ? unlisten() : (stop = unlisten)))
      .catch(() => {});
    return () => {
      unmounted = true;
      stop?.();
    };
  }, [qc]);
}

/** Действия вкладки «Замечания» (находки, замечания и предложения LLM): каждое возвращает свежее состояние подключения. */
export function useHealthActions(connectionId: string) {
  const qc = useQueryClient();
  const onSuccess = (v: HealthView) => qc.setQueryData(healthKey(connectionId), v);
  return {
    run: useMutation({ mutationKey: healthCheckKey(connectionId), mutationFn: () => api.runHealthCheck(connectionId), onSuccess }),
    ignore: useMutation({
      mutationFn: (p: { key: string; ignored: boolean }) => api.setFindingIgnored(connectionId, p.key, p.ignored),
      onSuccess,
    }),
    setStatus: useMutation({
      mutationFn: (p: { id: string; status: AnnotationStatus }) => api.setAnnotationStatus(connectionId, p.id, p.status),
      onSuccess,
    }),
    remove: useMutation({ mutationFn: (id: string) => api.deleteAnnotation(connectionId, id), onSuccess }),
    removeAll: useMutation({ mutationFn: () => api.deleteAllAnnotations(connectionId), onSuccess }),
    setProposalStatus: useMutation({
      mutationFn: (p: { id: string; status: ProposalAction; reason?: string }) => api.setProposalStatus(connectionId, p.id, p.status, p.reason ?? null),
      onSuccess,
    }),
    removeProposal: useMutation({ mutationFn: (id: string) => api.deleteProposal(connectionId, id), onSuccess }),
    removeAllProposals: useMutation({ mutationFn: () => api.deleteAllProposals(connectionId), onSuccess }),
  };
}
