/**
 * Квота «у предела»: использовано не меньше этой доли (спец. §8.2, правило `quota.nearLimit`: ≥ 90 % любого ресурса
 * ResourceQuota). Один порог для обзора проекта и вкладки «Квоты».
 */
export const QUOTA_NEAR_LIMIT = 0.9;

/** Порог в процентах — для текстов интерфейса. */
export const QUOTA_NEAR_LIMIT_PERCENT = Math.round(QUOTA_NEAR_LIMIT * 100);

/** Ресурс квоты у предела; доли нет (лимит не число) — нет. */
export const isNearLimit = (ratio: number | null | undefined): boolean => (ratio ?? 0) >= QUOTA_NEAR_LIMIT;
