#!/usr/bin/env python3
"""Сводка прогонов по метке: summarize.py <tag-prefix> [--answers]"""
import glob, json, os, sys
tag = sys.argv[1]; show = "--answers" in sys.argv
here = os.path.dirname(os.path.abspath(__file__))
files = sorted(glob.glob(os.path.join(here, "runs", f"*-{tag}-r*.json")))
for f in files:
    d = json.load(open(f, encoding="utf-8"))
    st = d["steps"]
    calls = [c for s in st for c in s["calls"]]
    errs = [c for c in calls if c["is_error"]]
    ct = sum(s["completion_tokens"] or 0 for s in st); llm = sum(s["llm_s"] for s in st)
    pt = max((s["prompt_tokens"] or 0) for s in st) if st else 0
    tools = ",".join(c["tool"] for c in calls)
    print(f'{os.path.basename(f)[:-12]:28} {d["total_s"]:6.0f}s steps={len(st):2} maxPrompt={pt:5} out={ct:5} {ct/llm if llm else 0:4.1f}tok/s errs={len(errs)} stop={d.get("stopped") or "-"}')
    print(f'   tools: {tools}')
    for c in errs:
        print(f'   ERR {c["tool"]} {json.dumps(c["args"], ensure_ascii=False)[:140]} -> {c["result_head"][:110]}')
    if show:
        print("   ANSWER:", (d.get("answer") or "(none)").replace("\n", " ")[:900])
