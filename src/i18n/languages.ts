/** Языки, для которых есть словарь `src/locales/<код>.json`. Остальные 10 языков добавляет этап O6. */
export type LanguageCode = "en" | "ru";

export const LANGUAGES: { code: LanguageCode; nativeName: string }[] = [
  { code: "en", nativeName: "English" },
  { code: "ru", nativeName: "Русский" },
];
