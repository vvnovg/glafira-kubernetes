"""Замер сервера модели: время до первого токена (обработка промпта) и скорость генерации.
Короткий промпт, затем промпт ~23 тыс. токенов дважды: второй раз с тем же префиксом показывает кэш.

    uv run --no-project --with openai==3.41.0 python bench.py http://127.0.0.1:11434/v1 qwen3.6-64k:35b-a3b
"""
import json, os, sys, time
from openai import OpenAI
base, model = sys.argv[1], sys.argv[2]
c = OpenAI(base_url=base, api_key=os.environ.get("LLM_API_KEY", "local"))
filler = " ".join(f"pod-{i} Running node-{i%7} restarts={i%5};" for i in range(1600))  # ~23 тыс. токенов
def run(prompt, max_tokens):
    t0 = time.time(); first = None; last = None
    s = c.chat.completions.create(model=model, messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens, temperature=0.2, stream=True, stream_options={"include_usage": True})
    usage = None
    for ch in s:
        if ch.usage: usage = ch.usage
        if ch.choices and (ch.choices[0].delta.content or getattr(ch.choices[0].delta, "reasoning_content", None) or getattr(ch.choices[0].delta, "reasoning", None)):
            if first is None: first = time.time()
            last = time.time()
    out = usage.completion_tokens if usage else None
    return {"ttft_s": round(first - t0, 2), "decode_tok_s": round((out - 1) / (last - first), 1) if out and last > first else None,
            "prompt_tokens": usage.prompt_tokens if usage else None, "completion_tokens": out, "total_s": round(time.time() - t0, 1)}
res = {}
res["short"] = run("Напиши 300 слов о Kubernetes.", 600)
res["long_cold"] = run(filler + "\nСколько подов перечислено? Ответь кратко.", 300)
res["long_warm"] = run(filler + "\nСколько подов перечислено? Ответь кратко.", 300)  # тот же префикс — кэш
print(json.dumps({"base": base, "model": model, **res}, ensure_ascii=False, indent=1))
