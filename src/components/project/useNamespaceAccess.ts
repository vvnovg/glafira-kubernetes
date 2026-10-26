import { type QueryClient, useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { useRefetchInterval } from "../../lib/tabActive";

/**
 * Как часто права перечитываются по таймеру, чтобы подвкладка не оставалась скрытой по устаревшим правам: срок кэша прав
 * в Rust (`ACCESS_TTL`, 5 минут) и ещё 5 с. Таймер зовёт команду без `force`: с интервалом ровно 5 минут тик приходил бы
 * чуть раньше, чем кэш истёк, получал бы закэшированные права, и до кластера доходил бы только каждый второй тик (раз в
 * 10 минут). С запасом каждый тик застаёт кэш истёкшим.
 */
export const ACCESS_REFRESH_MS = 5 * 60_000 + 5_000;

const accessKey = (connectionId: string, namespace: string) => ["access", connectionId, namespace] as const;

/** Права в проекте (спец. §5.4). Вкладки остаются смонтированными, а перечитывание при фокусе выключено, поэтому права обновляются по таймеру. */
export function useNamespaceAccess(connectionId: string, namespace: string) {
  // В скрытой вкладке права не перечитываются; на показанной они обновляются по таймеру или по «Обновить».
  const refetchInterval = useRefetchInterval(ACCESS_REFRESH_MS);
  return useQuery({
    queryKey: accessKey(connectionId, namespace),
    queryFn: () => api.getNamespaceAccess(connectionId, namespace),
    refetchInterval,
  });
}

/**
 * Ручное обновление прав: Rust забывает их кэш (`force`) и читает заново, результат ложится в тот же запрос, и
 * подвкладки перерисовываются. Сбой чтения запрос покажет сам: он помечается устаревшим и перечитывается.
 */
export async function refreshNamespaceAccess(queryClient: QueryClient, connectionId: string, namespace: string): Promise<void> {
  const queryKey = accessKey(connectionId, namespace);
  try {
    queryClient.setQueryData(queryKey, await api.getNamespaceAccess(connectionId, namespace, true));
  } catch {
    await queryClient.invalidateQueries({ queryKey });
  }
}
