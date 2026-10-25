import { render, screen } from "@testing-library/react";
import App from "./App";

test("показывает имя продукта", () => {
  render(<App />);
  expect(screen.getByText("Glafira for Kubernetes")).toBeInTheDocument();
});
