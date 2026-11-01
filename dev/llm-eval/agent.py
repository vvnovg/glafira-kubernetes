#!/usr/bin/env python3
"""Минимальный MCP-клиент для проверки Glafira с моделью на OpenAI-совместимом сервере (Ollama, vLLM).

Запускает `glafira-kubernetes mcp`, отдаёт модели схемы инструментов и инструкции Glafira, выполняет её вызовы
и пишет протокол прогона в runs/<сценарий>[-метка]-<время>.json: шаги, вызовы, ошибки, токены, время, ответ.

    uv run --no-project --with openai==3.41.0 --with mcp==2.3.0 python agent.py --scenario s1-health \
        --base-url http://127.0.0.1:11434/v1 --model qwen3.6-64k:35b-a3b
"""
import argparse
import asyncio
import json
import os
import sys
import time

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import Implementation
from openai import BadRequestError, OpenAI

HERE = os.path.dirname(os.path.abspath(__file__))
GLAFIRA = os.environ.get("GLAFIRA_BIN") or "/Applications/Glafira for Kubernetes.app/Contents/MacOS/glafira-kubernetes"

SYSTEM = (
    "You are a Kubernetes/OpenShift diagnostics assistant. You can only read the cluster through the provided "
    "tools of Glafira for Kubernetes. Call tools to gather facts before concluding; never invent objects, values "
    "or log lines. Never reveal masked values (***) or guess secrets. Answer the user in Russian, concisely, with "
    "the evidence (object names, states, messages) behind each conclusion."
)


def tool_specs(tools, only):
    return [
        {"type": "function", "function": {"name": t.name, "description": t.description or "", "parameters": t.input_schema}}
        for t in tools
        if not only or t.name in only
    ]


def result_text(res):
    text = "\n".join(c.text for c in res.content if getattr(c, "type", "") == "text")
    return "ERROR: " + text if res.is_error else text


async def run(args, scenario):
    client = OpenAI(base_url=args.base_url, api_key=os.environ.get("LLM_API_KEY", "local"))
    params = StdioServerParameters(command=GLAFIRA, args=["mcp"])
    log = {"scenario": scenario["id"], "prompt": scenario["prompt"], "model": args.model, "base_url": args.base_url,
           "steps": [], "started": time.time()}
    async with stdio_client(params) as (r, w):
        async with ClientSession(r, w, client_info=Implementation(name="glafira-llm-eval", version="0.1")) as s:
            init = await s.initialize()
            tools = (await s.list_tools()).tools
            specs = tool_specs(tools, set(args.tools.split(",")) if args.tools else None)
            log["tools"] = len(specs)
            system = SYSTEM + ("\n\nServer instructions:\n" + init.instructions if init.instructions else "")
            messages = [{"role": "system", "content": system}, {"role": "user", "content": scenario["prompt"]}]
            for step in range(args.max_steps):
                t0 = time.time()
                try:
                    resp = client.chat.completions.create(
                        model=args.model, messages=messages, tools=specs, temperature=args.temperature,
                        max_tokens=args.max_tokens,
                    )
                except BadRequestError as e:
                    # Например, переполнение контекста: это исход прогона, а не сбой агента.
                    log["answer"] = None
                    log["stopped"] = "llm_400: " + str(e)[:300]
                    break
                dt = time.time() - t0
                msg = resp.choices[0].message
                u = resp.usage
                entry = {"step": step, "llm_s": round(dt, 2),
                         "prompt_tokens": u.prompt_tokens if u else None,
                         "completion_tokens": u.completion_tokens if u else None,
                         "content": msg.content, "calls": []}
                messages.append(msg.model_dump(exclude_none=True))
                if not msg.tool_calls:
                    log["steps"].append(entry)
                    log["answer"] = msg.content
                    break
                for tc in msg.tool_calls:
                    try:
                        a = json.loads(tc.function.arguments or "{}")
                    except json.JSONDecodeError as e:
                        text, a = f"ERROR: arguments are not valid JSON: {e}", None
                    else:
                        text = result_text(await s.call_tool(tc.function.name, a))
                    entry["calls"].append({"tool": tc.function.name, "args": a, "result_bytes": len(text),
                                           "result_head": text[:300], "is_error": text.startswith("ERROR")})
                    messages.append({"role": "tool", "tool_call_id": tc.id, "content": text})
                log["steps"].append(entry)
                print(f"[{scenario['id']}] step {step}: {dt:.1f}s, {[c['tool'] for c in entry['calls']]}", file=sys.stderr)
            else:
                log["answer"] = None
                log["stopped"] = "max_steps"
    log["total_s"] = round(time.time() - log["started"], 1)
    return log


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", required=True)
    p.add_argument("--base-url", default="http://127.0.0.1:11434/v1")
    p.add_argument("--model", default=None, help="default: the first model the server lists")
    p.add_argument("--tools", default=None, help="comma-separated tool subset")
    p.add_argument("--max-steps", type=int, default=15)
    p.add_argument("--max-tokens", type=int, default=8192, help="reasoning tokens count towards it")
    p.add_argument("--temperature", type=float, default=0.2)
    p.add_argument("--var", action="append", default=[], help="KEY=VALUE for {KEY} in the prompt")
    p.add_argument("--tag", default="", help="suffix of the run file name")
    args = p.parse_args()
    scenarios = json.load(open(os.path.join(HERE, "scenarios.json"), encoding="utf-8"))
    sc = dict(next(x for x in scenarios if x["id"] == args.scenario))
    for kv in args.var:
        k, v = kv.split("=", 1)
        sc["prompt"] = sc["prompt"].replace("{" + k + "}", v)
    if args.model is None:
        args.model = OpenAI(base_url=args.base_url, api_key=os.environ.get("LLM_API_KEY", "local")).models.list().data[0].id
    log = asyncio.run(run(args, sc))
    os.makedirs(os.path.join(HERE, "runs"), exist_ok=True)
    out = os.path.join(HERE, "runs", f"{sc['id']}{'-' + args.tag if args.tag else ''}-{time.strftime('%H%M%S')}.json")
    json.dump(log, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"\n=== {sc['id']} ({log['total_s']} s, {len(log['steps'])} steps) → {out}\n")
    print(log.get("answer") or "(no answer)")


if __name__ == "__main__":
    main()
