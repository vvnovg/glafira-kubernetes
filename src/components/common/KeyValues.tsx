/** Метки или аннотации: `ключ=значение` по одному в строке; пусто — «—». */
export function KeyValues({ values }: { values: Record<string, string> }) {
  const entries = Object.entries(values);
  if (entries.length === 0) return <p className="muted">—</p>;
  return (
    <ul className="kv">
      {entries.map(([k, v]) => (
        <li key={k}>
          <code>
            {k}={v}
          </code>
        </li>
      ))}
    </ul>
  );
}
