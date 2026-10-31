/** Стандартный список имён полей маскирования — как `glafira_mcp_kubernetes::mask::DEFAULT_NAMES` (спец. §10.3); совпадение проверяет тест `standard_mask_fields_match_the_form` в src-tauri. */
export const STANDARD_MASK_FIELDS = [
  "password",
  "passwd",
  "secret",
  "token",
  "apikey",
  "api_key",
  "api-key",
  "credentials",
  "private",
  "authorization",
];
