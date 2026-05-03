---
name: write-docs
description: Write documentation in the Beyond voice. Use when writing API docs, guides, tutorials, READMEs, or reference material.
allowed-tools: Read, Glob, Grep, Edit, Write
model: claude-sonnet-4-6
---

## Before Writing

**Load the verbal identity:**

```
Read: .claude/skills/brand/verbal-identity.md
```

## The Only Rule That Matters

**Your job is to teach the reader how to do their job.**

Not to explain the system. Not to describe your architecture. Not to prove you thought about edge cases. The reader arrived with a goal. Get them there.

Every sentence earns its place by answering: _does this help the reader accomplish their goal faster?_ If not, cut it.

## Know What You're Writing

Different doc types serve different jobs. Mixing them produces mush.

### README.md — "What is this, and can it help me?"

The README is a **discovery document**. The reader just found this package and is deciding whether to go deeper. They have one job: evaluate fit.

**Structure:**

1. **One-line pitch** — What job does this do? Not what it is, what it _does for you_.
2. **Quick start** — The most common thing, working in under 60 seconds. Real code. No setup preamble.
3. **What else can it do** — Bullets, not paragraphs. Link out to guides for depth.
4. **Install** — Only if non-obvious.

**Rules:**

- The first sentence is a verb. "Authenticate users." "Manage sessions." Not "A library for..."
- The quick start must actually run. Test it.
- Don't explain your design decisions here. Nobody cares yet.
- No architecture diagrams, no philosophy, no history. Those live elsewhere.

**Example opening (correct):**

````markdown
# beyond/auth

Authenticate users, issue tokens, and manage sessions — deployed inside your network, owned by you.

## Quick Start

```sh
cargo add beyond-auth
```
````

```rust
let session = client.sessions().create(user_id).await?;
```

**Example opening (wrong):**

```markdown
# beyond/auth

beyond/auth is a comprehensive authentication library that provides a full suite of identity management features including session handling, token issuance, and user authentication built on modern cryptographic primitives...
```

---

### Guide — "How do I accomplish X?"

Guides are **task documents**. The reader has a specific job to do and needs a path through it. They don't want an overview — they want the steps.

**Structure:**

1. **Goal statement** — One sentence: what will the reader have when they're done?
2. **Prerequisites** — Only what's actually blocking. Not a generic "you should know Rust."
3. **Steps** — Numbered. One action per step. Code for every step that has code.
4. **What can go wrong** — Only real failure modes, not hypotheticals.

**Rules:**

- Title is a verb phrase: "Rotate a Signing Key", not "Signing Key Rotation"
- Every step has a clear output the reader can verify ("You should see `session_id` in the response")
- No background theory unless it's load-bearing for the task
- Don't explain what you're about to explain — just explain it

---

### API Reference — "What are my options for Y?"

Reference is a **lookup document**. The reader knows what they want to do — they just need the exact parameter name, the valid values, the shape of the response.

**Structure:**

1. **What it does** — One sentence, verb-first
2. **Request** — Method, path, auth requirements
3. **Parameters** — Table: name, type, required/optional, what it does
4. **Response** — Shape + example
5. **Errors** — Only the ones specific to this endpoint, not generic HTTP errors

**Rules:**

- No prose where a table works
- Example responses must be real (copy from tests if needed)
- Don't repeat information available elsewhere — link to it

---

## Voice

Neutral, precise, technical. Assume the reader is competent.

| Aspect      | Approach              |
| ----------- | --------------------- |
| Tone        | Neutral, direct       |
| Assumptions | Reader is a developer |
| Length      | Minimum viable        |
| Code:Prose  | More code, less prose |
| Warmth      | None needed           |

Don't explain what developers already know. Don't define HTTP. Don't say "simply" or "just" or "easy." Don't hedge with "you may want to" when you mean "do this."

## Before Publishing

1. What job is the reader trying to do? Does every section serve that job?
2. Can I cut the first paragraph entirely and lose nothing?
3. Is there a code example for every concept?
4. Would I be annoyed reading this as a developer in a hurry?
5. Does the first sentence tell them whether this doc is for them?
