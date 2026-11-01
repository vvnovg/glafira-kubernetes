# Local model

If your organization forbids sending data to external LLM services, the assistant can work with a model inside your
perimeter: on an organization server or right on the engineer's computer. Glafira does not change for this: it stays the
MCP server (see [LLM assistants (MCP)](llm-mcp.md#how-it-works)); only the model the client talks to changes. This
chapter covers where to run the model, which models and clients have been tested with Glafira, ready-to-use settings,
and how to check the setup without a network.

## Where the model runs {#where}

- **External service** (Claude, GPT and others) — the simplest and strongest option if your organization's policy
  allows it. Client setup is in [LLM assistants (MCP)](llm-mcp.md#connect-client).
- **Organization model server in the internal perimeter** — a GPU server shared by the team with an OpenAI- or
  Anthropic-compatible API. The client is set up the same way as for a local model below, only with that server's
  address and key. Auditing and tuning the model server itself is done by a separate tool of the Glafira family.
- **Model on the engineer's computer** — when external services are forbidden and there is no server in the perimeter
  yet, or it is being built and tuned. Cluster data never leaves the computer: the model, the client and Glafira all run
  on it.

The rest of the chapter is mostly about the third option; the client settings fit the second one as well.

## Hardware {#hardware}

The model needs fast memory: the whole model and its context must fit into video memory or, on Apple Silicon, into
unified memory. On every step the assistant sends the model the Glafira tool schemas (about 9 thousand tokens) and the
whole conversation, so you need a context of at least 32 thousand tokens, preferably 64 thousand.

On modest hardware, MoE models with few active parameters win (for example, Qwen3.6-35B-A3B: 35 billion parameters, 3
billion of them active per token): speed depends on the active parameters, not on the size. A dense model of the same
class (Qwen3.8-27B) answers more precisely but 3–4 times slower.

- **macOS on Apple Silicon** — the Ollama server. 48 GB or more: Qwen3.6-35B-A3B; 32 GB: gpt-oss-20b, with 35B-A3B at
  the limit; 16–24 GB is not enough for cluster analysis. Tested on an M4 Pro with 48 GB.
- **Linux with an NVIDIA GPU** — vLLM or Ollama. 24 GB of video memory fits Qwen3.6-35B-A3B or Qwen3.8-27B at 4 bits
  with a limited context, 16 GB fits gpt-oss-20b. vLLM is the same server an organization usually runs in its
  perimeter, so this is a good rehearsal of the second option. Not tested.
- **Windows with an NVIDIA GPU** — Ollama; vLLM only runs under WSL2. Not tested.
- **A computer without a discrete GPU** — only MoE models on the CPU (Ollama) with 32–64 GB of DDR5; expect noticeably
  longer waits. Not tested.
- **An Intel Mac** is not suitable.

## Tested models {#models}

Five tasks on a test cluster: find the problem pods of a project, explain a pod crash from its log (without revealing
the password in the log), explain a Pending pod, find a log line written an hour and a half ago, suggest requests and
limits. Mac M4 Pro, 48 GB, 4-bit models:

- **Qwen3.6-35B-A3B** — recommended. All tasks correct, 10–65 seconds per task in Ollama.
- **Qwen3.8-27B** — all tasks correct, the most detailed answers (it checks node resources, taints, LimitRange), but
  2–11 minutes per task.
- **gpt-oss-20b** — fast and light on memory, solved four tasks out of five; on the log search it overflowed the 64
  thousand token context with a large answer.
- **Qwen3-30B-A3B-Instruct** — solved four tasks out of five: when searching for a log line it looked for a substring
  instead of an exact match.

No model revealed masked values. All models sometimes call a tool with wrong arguments and correct themselves from
Glafira's answer.

## Model in Ollama {#ollama}

Ollama (https://ollama.com) is an open-source model server for macOS, Windows and Linux; on Apple Silicon it runs on MLX
and in our measurements was 1.3–3 times faster than vLLM with vllm-metal. One address serves both the OpenAI API (for
Qwen Code) and the Anthropic API (for Claude Code).

Ollama's default context is smaller than needed: without configuration a long request is silently truncated. The most
reliable way is a separate model with the right context; it works the same for every client and takes almost no disk
space:

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

The model takes about 23 GB. After 5 minutes without requests Ollama unloads it from memory, and the next answer starts
about 10 seconds later; the variable `OLLAMA_KEEP_ALIVE` sets how long it stays loaded. In a closed perimeter, download
the model on a computer with internet access and copy the Ollama models directory (`~/.ollama/models`), or deploy it from
an internal mirror.

## Qwen Code {#qwen-code}

Qwen Code (https://github.com/QwenLM/qwen-code) is an open-source command-line assistant (Apache 2.0) designed for Qwen
models. It is installed from npm (in a closed perimeter, from an internal mirror); version 0.26.5 was tested:

```bash
npm install -g @qwen-code/qwen-code@0.26.5
```

**Put the settings in place before the first launch.** Without them Qwen Code updates itself on launch (outbound traffic
and an untested version) and turns on the `auto` approval mode, in which a model first assesses every tool call — an
extra request on every step. User settings live in `~/.qwen/settings.json` (on Windows
`%USERPROFILE%\.qwen\settings.json`); an administrator can deploy them as system settings:

```json
{
  "env": { "LOCAL_LLM_API_KEY": "ollama" },
  "modelProviders": {
    "openai": [
      {
        "id": "qwen3.6-64k:35b-a3b",
        "name": "Qwen3.6-35B-A3B, 64k context (local Ollama)",
        "envKey": "LOCAL_LLM_API_KEY",
        "baseUrl": "http://127.0.0.1:11434/v1",
        "generationConfig": {
          "timeout": 600000,
          "streamIdleTimeoutMs": 600000,
          "maxRetries": 1,
          "contextWindowSize": 65536,
          "samplingParams": { "temperature": 0.2, "max_tokens": 8192 }
        }
      }
    ]
  },
  "security": { "auth": { "selectedType": "openai" } },
  "model": { "name": "qwen3.6-64k:35b-a3b" },
  "privacy": { "usageStatisticsEnabled": false },
  "general": { "enableAutoUpdate": false },
  "telemetry": { "enabled": false },
  "tools": {
    "approvalMode": "default",
    "disabled": [
      "run_shell_command", "exec", "edit", "write_file", "notebook_edit",
      "web_fetch", "web_search", "image_gen",
      "agent", "create_sub_session", "list_agents", "task_stop", "send_message",
      "task_create", "task_update", "task_list",
      "team_create", "team_delete", "team_plan_approval", "request_shutdown",
      "cron_create", "cron_list", "cron_delete", "loop_wakeup", "monitor",
      "autofix_pr", "enter_worktree", "exit_worktree", "workflow",
      "artifact", "record_artifact", "record_source", "report_findings",
      "skill", "save_memory", "manage_memory", "search_memory",
      "lsp", "advisor", "get_goal", "update_goal", "zoom_image",
      "enter_plan_mode", "exit_plan_mode", "list_directory", "glob"
    ]
  },
  "mcpServers": {
    "glafira-kubernetes": {
      "command": "/Applications/Glafira for Kubernetes.app/Contents/MacOS/glafira-kubernetes",
      "args": ["mcp"],
      "timeout": 120000,
      "trust": true,
      "alwaysLoadTools": true
    }
  }
}
```

What matters here:

- `envKey` is the name of the variable that holds the key, not the key itself; Ollama does not check the key. For an
  organization model server, put its address in `baseUrl` and its key in the variable.
- `contextWindowSize` goes inside the model entry: general generation settings do not apply to models from
  `modelProviders`.
- `tools.disabled` removes built-in tools that cluster analysis does not need: above all the shell
  (`run_shell_command`), file writing and the web. With a shell the model could bypass Glafira and run `kubectl` with
  your kubeconfig — without masking and without the MCP log. Fewer tools also mean a shorter request on every step and a
  faster answer. File reading and search are kept (Qwen Code saves a large tool result to a file, and the model reads it
  from there), as are questions to the user and the task list.
- `alwaysLoadTools` loads the Glafira tools at once; without it the model spends a step looking up each tool before its
  first call. `trust` allows the Glafira tools without confirmation in Qwen Code: they cannot change the cluster (see
  [LLM assistants (MCP)](llm-mcp.md#what-llm-sees)).
- The path to Glafira is the one in the [[ui:settings.mcp.otherClient]] snippet in [[ui:settings.mcp.section]].

Glafira sees Qwen Code as the client `qwen-cli-mcp-client-glafira-kubernetes` and asks you to approve it on the first
connection (see [Approve a client](llm-mcp.md#approve)): start `qwen` interactively and approve the client in the
Glafira window. If the MCP server is defined not in the user settings but in a project folder's `.qwen/settings.json`,
Qwen Code starts it only after `qwen mcp approve glafira-kubernetes`, and any change to the server entry resets that
approval.

## Claude Code {#claude-code}

Claude Code works with a local model through the Anthropic-compatible address of Ollama (or vLLM). It needs no separate
model setup, only environment variables:

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:11434
export ANTHROPIC_API_KEY=ollama
export ANTHROPIC_AUTH_TOKEN=ollama
export ANTHROPIC_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_OPUS_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_SONNET_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_HAIKU_MODEL=qwen3.6-64k:35b-a3b
export CLAUDE_CODE_SUBAGENT_MODEL=qwen3.6-64k:35b-a3b
export CLAUDE_CODE_MAX_CONTEXT_TOKENS=65536
export CLAUDE_CODE_MAX_OUTPUT_TOKENS=8192
export ENABLE_TOOL_SEARCH=false
export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
export DISABLE_TELEMETRY=1
export DISABLE_ERROR_REPORTING=1
```

- The address has no `/v1`. All model tiers and subagents point to the local model; otherwise Claude Code calls models
  the server does not have.
- `CLAUDE_CODE_MAX_CONTEXT_TOKENS` gives the real context size: Claude Code cannot tell it from the model name and logs
  `unrecognized_model` — this is not an error.
- `ENABLE_TOOL_SEARCH=false` loads the Glafira tools at once, like `alwaysLoadTools` in Qwen Code.
- `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC` turns off updates, telemetry, error reports and other outbound calls. With a
  key in `ANTHROPIC_API_KEY` no account sign-in is needed.
- The Glafira server is added as usual (see [Connect a client](llm-mcp.md#connect-client)). Narrow the built-in tools
  down to file reading: `claude --tools "Read,Grep"`; the Glafira tools can be allowed without prompts:
  `--allowedTools "mcp__glafira-kubernetes"`.
- In non-interactive mode (`claude -p`), redirect input from `/dev/null`, otherwise Claude Code waits for input.

Claude Code connects to Glafira as the client `claude-code`.

## Organization model server {#internal-server}

If your organization has its own model server, only the address, key and model name change in the client: in Qwen Code
`baseUrl`, the key variable and `id`; in Claude Code `ANTHROPIC_BASE_URL`, `ANTHROPIC_API_KEY` and the model names (a
model name must not contain `/`). The rest of the settings above stay the same.

If the server is vLLM, it must have tool-call parsing enabled (`--enable-auto-tool-choice` and the
`--tool-call-parser` matching the model, `qwen3_coder` for Qwen3.6) and a context of at least 64 thousand tokens
(`--max-model-len`). If the model's reasoning is on, it uses the same output token limit as the answer itself: a limit of
8 thousand tokens is safer than 4 thousand.

## Checking the setup {#smoke}

Check the setup once after configuring it, and after changing the model, the client or the Glafira version:

1. Start Glafira, enable MCP and give the connection LLM access (see [LLM assistants (MCP)](llm-mcp.md#enable)).
2. Start the client and ask: "Show the Glafira connections". Approve the client in the Glafira window; the assistant
   should list the connections with LLM access.
3. Ask typical questions about a project: "What is wrong in project …? Find the problem pods and explain the causes",
   "Why does pod … crash? Look at the log", "Why is pod … Pending?". The answers should rely on cluster objects and
   messages.
4. If a log contains a password or a token, make sure the assistant sees and shows `***`.
5. In [[ui:settings.mcp.section]], open the MCP log: it shows the assistant's calls (see
   [Sessions, revoking and the log](llm-mcp.md#sessions-log)).

To check without a network on macOS, repeat the check in a sandbox that forbids all outbound connections except the
computer itself (localhost) and local sockets, which Ollama and Glafira use. A profile, for example `offline.sb`:

```
(version 1)
(allow default)
(deny network-outbound)
(allow network-outbound (remote ip "localhost:*"))
(allow network-outbound (remote unix-socket))
```

```bash
sandbox-exec -f offline.sb curl -sS -m 5 https://ollama.com          # must fail to connect
sandbox-exec -f offline.sb qwen "What is wrong in project team-a? Find the problem pods"
sandbox-exec -f offline.sb claude -p "What is wrong in project team-a? Find the problem pods" < /dev/null
```

If the client answers the same way as with a network, it needs nothing but the local model and Glafira. Glafira's
developers check models with the script in `dev/llm-eval` of the repository: five tasks, logs and timings.

## If it does not work {#troubleshooting}

- The answer is cut off or the model "forgets" the start of the conversation — the model's context is too small: check
  `num_ctx` in Ollama ([Model in Ollama](local-llm.md#ollama)) and `contextWindowSize` or
  `CLAUDE_CODE_MAX_CONTEXT_TOKENS` in the client.
- The answer is empty or stops mid-thought — the model's reasoning used up the output token limit: raise it to 8
  thousand.
- Glafira answers `invalid type: string, expected struct` — the model server passed a nested argument as a string.
  Glafira versions that publish tool schemas without `$ref` do not trigger this; update Glafira.
- The model keeps retrying similar calls with errors — this happens with small models; use a larger model or
  Qwen3.6-35B-A3B.
- The first answer after a break takes longer than usual — Ollama unloaded the model from memory; see
  `OLLAMA_KEEP_ALIVE`.
- Qwen Code asks you to choose a provider on launch — it did not find `~/.qwen/settings.json` with `modelProviders` and
  `security.auth.selectedType`.
- The Glafira tools are missing in Qwen Code — `qwen mcp list` shows `Pending approval`: run
  `qwen mcp approve glafira-kubernetes`.
- `client_not_approved` — approve the client in the Glafira window while it is connected (see
  [Approve a client](llm-mcp.md#approve)).
