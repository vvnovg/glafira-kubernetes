import type { QueryClient } from "@tanstack/react-query";
import { useConnectionEpoch } from "../state/connectionEpoch";

/**
 * После входа заново открытые вкладки подключения перечитываются (спец. §5.2): запросы с этим
 * подключением (ключ `[вид, connectionId, …]`) помечаются устаревшими, watch и логи перезапускаются.
 * Сведения о подключении (`["connection", id]`) уже свежие — их вход и вернул.
 */
export function afterSignIn(queryClient: QueryClient, connectionId: string): void {
  void queryClient.invalidateQueries({ predicate: (q) => q.queryKey[0] !== "connection" && q.queryKey[1] === connectionId });
  useConnectionEpoch.getState().bump(connectionId);
}
