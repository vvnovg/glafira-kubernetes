import { render, screen } from "@testing-library/react";
import { vi } from "vitest";

vi.mock("./api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("./api/client")>();
  return {
    ...orig,
    api: {
      getLanguageSettings: vi.fn(() => new Promise(() => {})),
      listConnections: vi.fn().mockResolvedValue([]),
      listFolders: vi.fn().mockResolvedValue([]),
      getMcpStatus: vi.fn().mockResolvedValue(mcpStatus()),
      checkUpdate: vi.fn().mockResolvedValue({ status: "upToDate" }),
    },
  };
});

import { mcpStatus } from "./test/mcp";
import App from "./App";

test("пустое окно предлагает добавить подключение", async () => {
  render(<App />);
  expect(await screen.findByText(/Добавьте первое подключение/)).toBeInTheDocument();
  expect(screen.getByText("Выберите подключение слева или добавьте новое.")).toBeInTheDocument();
});
