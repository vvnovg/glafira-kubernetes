import { create } from "zustand";

/**
 * Номер сессий подключения: растёт после каждого успешного входа заново (токен, браузер,
 * exec-плагин) и всякий раз, когда Rust останавливает сессии подключения без события —
 * сохранение профиля, вставленный токен, обновление импортом (`restartSessions`). По нему
 * открытые вкладки перезапускают watch и логи.
 */
interface EpochState {
  epochs: Record<string, number>;
  bump: (connectionId: string) => void;
}

export const useConnectionEpoch = create<EpochState>((set) => ({
  epochs: {},
  bump: (id) => set((s) => ({ epochs: { ...s.epochs, [id]: (s.epochs[id] ?? 0) + 1 } })),
}));

export function useEpoch(connectionId: string): number {
  return useConnectionEpoch((s) => s.epochs[connectionId] ?? 0);
}
