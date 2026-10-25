import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useLanguageSync } from "./lib/useLanguageSync";

function LanguageSync() {
  useLanguageSync();
  return null;
}

// Без автоповтора: backend сам ждёт до таймаута и быстро отказывает при недоступном кластере,
// а повтор удваивал бы ожидание. Повторить можно кнопкой в баннере ошибки.
const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <LanguageSync />
      <main className="page">Glafira for Kubernetes</main>
    </QueryClientProvider>
  );
}
