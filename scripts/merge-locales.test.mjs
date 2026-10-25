import assert from "node:assert/strict";
import { test } from "node:test";
import { deepMerge } from "./merge-locales.mjs";

test("сливает вглубь и не перезаписывает без --replace", () => {
  const base = { common: { save: "Save" } };
  assert.deepEqual(deepMerge(base, { common: { cancel: "Cancel" }, tabs: { help: "Help" } }), []);
  assert.deepEqual(base, { common: { save: "Save", cancel: "Cancel" }, tabs: { help: "Help" } });
  assert.deepEqual(deepMerge(base, { common: { save: "Store" } }), ["common.save: «Save» → «Store»"]);
  assert.equal(base.common.save, "Save");
  assert.deepEqual(deepMerge(base, { common: { save: "Store" } }, true), []);
  assert.equal(base.common.save, "Store");
  assert.deepEqual(deepMerge(base, { common: { save: "Store" } }), [], "то же значение — не конфликт");
  assert.deepEqual(deepMerge(base, { common: { save: { x: "y" } } }), ["common.save: строка → объект"]);
});
