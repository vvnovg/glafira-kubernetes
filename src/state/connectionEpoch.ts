import { create } from "zustand";

/**
 * Номер входа подключения: растёт после каждого успешного входа заново (токен, браузер,
 * exec-плагин). По нему открытые вкладки перезапускают watch и логи.
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
