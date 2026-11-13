# Getting Started

## Minimum Requirements

Before using the AWS Advanced Go Wrapper, you must install:

- GoLang 1.26.0

If you are using the AWS Advanced Go Wrapper as part of a Go project, include one of the wrapper's available drivers as a dependency.

> [!NOTE]
> Depending on which features of the AWS Advanced Go Wrapper you use, you may have additional requirements. Please refer to this [table](./user-guide/UsingTheGoWrapper.md#plugins) for more information.

## Obtaining the AWS Advanced Go Wrapper

You can use `go get` to obtain the AWS Advanced Go Wrapper.

To obtain the Aws Advanced Go Wrapper pgx driver:
```
go get github.com/aws/aws-advanced-go-wrapper/pgx-driver@latest
```

To obtain the Aws Advanced Go Wrapper Go-MySQL-Driver driver:
```
go get github.com/aws/aws-advanced-go-wrapper/mysql-driver@latest
```

To obtain the Aws Advanced Go Wrapper Bun pgdriver driver:
```
go get github.com/aws/aws-advanced-go-wrapper/bun-pg-driver@latest
```

Each driver module registers itself when it is imported, so import it for its side effects:

```go
import (
    _ "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
)
```

## Using the AWS Advanced Go Wrapper

For more detailed information about how to use and configure the AWS Advanced Go Wrapper, please visit [this page](./user-guide/UsingTheGoWrapper.md).

## Configuring the Wrapper with AI Tools

If you use an AI coding assistant (Kiro, Cursor, Claude Code, GitHub Copilot, ChatGPT, Amazon Bedrock, etc.), you can use the wrapper's configuration assistant skill to help build, review, or troubleshoot your wrapper configuration.

The skill is a single self-contained markdown file: [`GO-WRAPPER-CONFIGURATION-ASSISTANT.md`](./GO-WRAPPER-CONFIGURATION-ASSISTANT.md). It covers every plugin, parameter, default value, dialect, failover mode, and compatibility constraint of the 2026-10-09 release, which modules and versions each feature needs, and recipes for `database/sql` with the pgx and MySQL driver modules and for Bun, so the AI can answer configuration questions without access to the wrapper source.

The file follows the [Agent Skills](https://agentskills.io) format. It is large: about 132,000 tokens on Claude Opus 5.5 (other models count tokens differently), and the AI receives it again with every message. Tools that cache the system prompt handle it far more cheaply than tools that resend it in full each time.

### Quick start

1. Download or copy [`GO-WRAPPER-CONFIGURATION-ASSISTANT.md`](./GO-WRAPPER-CONFIGURATION-ASSISTANT.md).
2. Add it to your AI tool:
   - **Tools that support Agent Skills** (Kiro, Cursor, Claude Code, GitHub Copilot in VS Code, and others): create a folder named `go-wrapper-configuration-assistant` in one of the tool's skills directories, and save the file inside it as `SKILL.md`. The folder name must match the `name` field at the top of the file; otherwise the tool does not load the skill.

     | Tool | Project skills directory | User skills directory |
     |---|---|---|
     | Kiro | `.kiro/skills/` | `~/.kiro/skills/` |
     | Cursor | `.cursor/skills/`, `.agents/skills/`, or `.claude/skills/` | `~/.cursor/skills/`, `~/.agents/skills/`, or `~/.claude/skills/` |
     | Claude Code | `.claude/skills/` | `~/.claude/skills/` |
     | GitHub Copilot in VS Code | `.github/skills/`, `.agents/skills/`, or `.claude/skills/` | `~/.copilot/skills/`, `~/.agents/skills/`, or `~/.claude/skills/` |

     For example, to add the skill to a project for Claude Code, Cursor, and VS Code at once:

     ```bash
     mkdir -p .claude/skills/go-wrapper-configuration-assistant
     curl -fsSL -o .claude/skills/go-wrapper-configuration-assistant/SKILL.md \
       https://raw.githubusercontent.com/aws/aws-advanced-go-wrapper/main/docs/GO-WRAPPER-CONFIGURATION-ASSISTANT.md
     ```

   - **Claude Projects**: upload it as a project knowledge document.
   - **ChatGPT custom GPT**: upload it as a knowledge file.
   - **Amazon Bedrock**: send the file as the system prompt of your `InvokeModel` or `Converse` request. Avoid attaching it to a Bedrock knowledge base: a knowledge base passes the model only the passages that match each question, so the skill's interview steps and rules (section 1 of the file) may be missing.
   - **Plain chat (ChatGPT, Claude, Gemini)**: paste it at the start of a new conversation, then ask your question.
3. Ask the AI for help. It will interview you when it needs more details, or give you a configuration straight away if you ask it to skip the interview.

### Example prompts

The simplest way to start:

```
Help me configure the AWS Advanced Go Wrapper for my application.
```

Skip the interview when you already know what you want:

```
Give me a default configuration for Aurora PostgreSQL with database/sql, the pgx driver, and failover support.
```

Review an existing configuration:

```
Review this wrapper configuration and tell me what to change:

<paste your imports, DSN, and sql.Open call here>
```

Diagnose an error:

```
After an Aurora failover my Go service gets "The active SQL connection has changed due to a
connection failure. Please re-configure session state if required." What should my code do?
```

The skill targets the 2026-10-09 release and avoids guessing: if it doesn't know something, it says so and points you at the official docs.
