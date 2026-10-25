import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Layout } from "./components/Layout";
import { useAppMenu } from "./lib/useAppMenu";
import { useLanguageSync } from "./lib/useLanguageSync";

function LanguageSync() {
  useLanguageSync();
  return null;
}

/** Меню Help на macOS (на других системах ничего не делает). */
function AppMenu() {
  useAppMenu();
  return null;
}

// Без автоповтора: backend сам ждёт до таймаута и быстро отказывает при недоступном кластере,
// а повтор удваивал бы ожидание. Повторить можно кнопкой в баннере ошибки.
// Без перезапроса при возвращении сети: вкладки остаются смонтированными, и событие online
// заново запустило бы подключение каждой открытой вкладки (exec-плагин или вход в браузере
// у профилей с истекающими учётными данными) без действия пользователя.
const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false, refetchOnReconnect: false } },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <LanguageSync />
      <AppMenu />
      <Layout />
    </QueryClientProvider>
  );
}
