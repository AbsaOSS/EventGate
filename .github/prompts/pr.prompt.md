---
mode: agent
description: Generate comprehensive pull request description following industry best practices
---

Your task is to create a comprehensive PR description from the current branch to the master branch and create the PR on the GitHub side using GitHub MCP server.

## Instructions
Analyze the commit messages and code changes from the current branch, then generate a well-structured PR description following the template below.

## PR Description Template

Use this exact structure and format for the output:

```markdown
<!-- What problem does it solve or what feature does it add? -->
## Overview

<!-- Summarize the key changes for release notes. -->
## Release Notes
- TBD: 1st item of release notes
- TBD: 2nd item of release notes

<!-- Add attached issue that this PR solves. -->
## Related
Closes #issue_number
```

## Guidelines
- Be explicit and use complete sentences with active voice
- Keep descriptions concise but informative - aim for clarity over brevity
- Focus on helping the reviewer understand both the technical changes and business context
- Use conversational, prose-like language rather than terse fragments
- If referencing tickets, explain the change first, then reference the ticket
- Highlight any security, performance, or architectural considerations

## Follow-up steps
- Create the PR on GitHub using the generated description
- Assign appropriate reviewers and labels based on the repository's contribution guidelines
