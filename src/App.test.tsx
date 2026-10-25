import { render, screen } from "@testing-library/react";
import { vi } from "vitest";

vi.mock("./api/client", () => ({
  api: { getLanguageSettings: vi.fn(() => new Promise(() => {})) },
}));

import App from "./App";

test("показывает имя продукта", () => {
  render(<App />);
  expect(screen.getByText("Glafira for Kubernetes")).toBeInTheDocument();
});
