# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a disk-backed cache implementation project for Pydantic objects, featuring a two-tier LRU cache with in-memory and SQLite-backed persistent storage.

The project follows the specification in `spec.md`.

### **Implementing steps**
All these MUST be followed:
- Implementation MUST follow the sequence in `IMPLEMENTATION_PLAN.md`
- **Requirements source**: Get detailed behavior requirements from `spec.md`, NOT from IMPLEMENTATION_PLAN.md
- Implement steps in order
- **Implement EXACTLY ONE step at a time - NEVER implement multiple steps together, even if they seem related or similar**
- 🚨 **ONE STEP ONLY: Even if steps 7 & 8 are both "Delete operations", implement them separately**. **Even if it feels inefficient or slow, ONE STEP AT A TIME is mandatory.** **No exceptions. No batching. No grouping. ONE STEP ONLY.**
- **NO BATCHING: Do not group steps together for "efficiency" - this violates the one-step rule**
- Implementation of "one step" includes: planning, implementing code and tests, running validations, and committing
- Update progress in `IMPLEMENTATION_PLAN.md`. After each step, mark it as done.
- Never skip steps.
- You are allowed to finish implementing the plan, even if you need to compact the conversation.

Do not stop between steps to ask for user confirmation or feedback, unless you encounter a problem you cannot solve.

### **Commit Message Format**

Keep commit messages concise:
```
Step X: [Short title]

[One line summary of changes]

🤖 Generated with Claude Code
```
- DO NOT specify "Co-Authored-By"

Example:
```
Step 12: Memory Size Tracking

Add size tracking for in-memory cache items.

🤖 Generated with Claude Code
```

### **How to implement each step**

**Work through these 3 sub-steps sequentially:**

1. **Plan** - Read `spec.md` for this step's requirements. Describe the behavior to implement in detail. Design tests for the new behavior. Identify if any existing tests need updates to cover modified functionality.

2. **Implement** - Write the code and all tests together. Include updates to existing tests if identified in planning.

3. **Validate and commit** - Run `./devtools/run_all_agent_validations.sh` and fix any failures. Repeat until all validations pass (zero errors, zero warnings). Once passing, commit the work and update `IMPLEMENTATION_PLAN.md`.

**Progress Tracking:**
- Use clear markdown comments to show progress (e.g., `## Step 5 Plan: SQLite Connection Setup`)
- Do NOT use TodoWrite for step tracking (wastes tokens)
- Only use TodoWrite if a single step has >5 complex sub-tasks or user explicitly requests it

## Development Guidelines

### **Validation Strategy**

Run `./devtools/run_all_agent_validations.sh` during the "Validate and commit" sub-step.

🚨 **ABSOLUTE ZERO TOLERANCE POLICY** 🚨
- ZERO test failures
- ZERO linting errors
- ZERO type errors
- ZERO warnings

### **When Validation Fails**

**Critical: Think before changing tests**

When fixing validation failures, follow this process:

1. **Identify the failure** - What exactly is failing? (test, type check, lint, etc.)

2. **Check the spec first** - Before changing ANY code or test:
   - Re-read the relevant section of `spec.md`
   - Confirm what the correct behavior should be
   - Verify your understanding matches the spec

3. **Determine the fix**:
   - If test fails because **code is wrong**: Fix the code to match spec
   - If test fails because **test expectation is wrong**: Verify against spec, then fix test
   - **Never** change test expectations just to make tests pass without re-confirming against spec

4. **Common mistake to avoid**:
   - ❌ Test fails → change test expectation → test passes → commit
   - ✅ Test fails → check spec → fix code to match spec → test passes → commit

5. **Apply fix and re-run** - Run validations again until passing

**Rule: Both code AND tests must match the spec. If validation passes and both match spec, commit immediately.**

### **🚫 FORBIDDEN PHRASES 🚫**

**NEVER SAY:**
- "Some tests are skipped but acceptable"
- "Only minor warnings remain"
- "Most tests pass"
- "Timing issues are expected"
- "This is acceptable"
- "Tests pass in isolation but fail in parallel"

**✅ ONLY 2 ACCEPTABLE OUTCOMES ✅**
- All tests and validations pass
- You've tried to fix the errors and failed (then tell the user)

## **Core Development Principles**
- **One Change = One Complete, testable Capability**
- **Validation-Driven Development** (every change must pass all validations)
- **Test-First Implementation** (behavior tests that target the API of the package are Priority 1)
- **No Layer-Based Changes**. Complete capabilities only - DO NOT implement multiple distinct capabilities in the same change. DO implement a capability and all its tests before continuing to the next capability.
- **Before committing, make sure all new or changed behaviors have tests that cover them well**

### **Small parts principle**
Prefer breaking down functionality into small capabilities that are individually testable.
In addition to the external API-level tests, also create tests for the smaller capabilities.
For example, a serialization and deserialization capability based on the model type is individually testable.

### **Testing guidelines**
See the tests/CLAUDE.md file
