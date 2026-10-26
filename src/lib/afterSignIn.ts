import type { QueryClient } from "@tanstack/react-query";
import { useConnectionEpoch } from "../state/connectionEpoch";

/**
 * Watch и логи подключений перезапускаются (номер входа растёт). Rust останавливает сессии подключения,
 * когда меняется его профиль или токен: сохранение формы, вставленный токен, обновление импортом
 * (`stop_for`). Остановленная сессия заканчивается без события, и открытый список остался бы «живым»
 * без данных. Запросы здесь не трогаются: после сохранения и импорта их сбрасывает вызывающий,
 * после входа — `afterSignIn`.
 */
export function restartSessions(connectionIds: Iterable<string>): void {
  const { bump } = useConnectionEpoch.getState();
  for (const id of connectionIds) bump(id);
}

/**
 * После входа заново открытые вкладки подключения перечитываются (спец. §5.2): запросы с этим
 * подключением (ключ `[вид, connectionId, …]`) помечаются устаревшими, watch и логи перезапускаются.
 * Сведения о подключении (`["connection", id]`) уже свежие — их вход и вернул.
 */
export function afterSignIn(queryClient: QueryClient, connectionId: string): void {
  void queryClient.invalidateQueries({ predicate: (q) => q.queryKey[0] !== "connection" && q.queryKey[1] === connectionId });
  restartSessions([connectionId]);
}
