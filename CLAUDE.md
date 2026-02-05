# CLAUDE.md

## Git Workflow

- **Always create PRs** - Never push directly to main.
- **PR creation process**:
  1. Create a branch: `git checkout -b feature/short-description` or `fix/short-description`
  2. Make changes and commit
  3. Push: `git push -u origin <branch>`
  4. Create PR: `gh pr create --title "..." --body "..."`
- **Merge before deleting** - Use `gh pr merge` before `git branch -d` to avoid orphaned PRs.
- **Branch naming**: `feature/`, `fix/`, `docs/` prefixes based on the type of change.
- **Commit messages**: Short imperative subject line, details in body if needed.

## Issues for Future Work

- **Create issues for ideas/tasks to come back to later**: `gh issue create --title "..." --body "..."`
- **List open issues**: `gh issue list`
- **Work on an issue**: Reference it in the PR with "Fixes #123" or "Closes #123" in the PR body to auto-close when merged.

## Code Standards

- **Minimize dependencies** - Use Python stdlib when possible. Avoid adding numpy, pandas, or heavy libraries for simple operations.
- **Python 3.10+** - Use modern syntax (type hints, `|` for unions, walrus operator where clear).
- **No emojis in code** - Unless explicitly requested.

## Project Structure

This is an Amazon SQP (Search Query Performance) keyword tracker:

```
sqp_analyzer/
├── commands/           # CLI entry points
│   ├── quarterly_tracker.py  # Main quarterly tracking
│   ├── fetch_sqp_data.py     # Fetch SQP reports from SP-API
│   ├── fetch_listing.py      # Fetch listing content
│   └── fetch_traffic_sales.py
├── sheets/client.py    # Google Sheets integration
├── models.py           # Data models
└── config.py           # Environment config
```

## Common Commands

```bash
# Quarterly tracker
python -m sqp_analyzer.commands.quarterly_tracker --start --asin B0XXX
python -m sqp_analyzer.commands.quarterly_tracker --update-all

# Fetch SQP data
python -m sqp_analyzer.commands.fetch_sqp_data --asin B0XXX --wait
```

## Session Best Practices (from Insights)

- **Provide detailed plans upfront** - Sessions with explicit step-by-step plans achieve better outcomes.
- **Let Claude finish planning** - Don't interrupt during planning phases. Let it complete, then redirect if needed.
- **Define completion criteria** - Be explicit about what "done" looks like before starting.
- **Group related changes** - Keep implementation → test → PR in one session rather than splitting across sessions.
- **Break long tasks into checkpoints** - For multi-step work, define save points so progress isn't lost.

## When Working on This Project

1. **Verify imports** after any file changes - run `python -c "from sqp_analyzer.commands.quarterly_tracker import main"` to check.
2. **SQP reports take 30-60 min** - Don't wait synchronously in tests.
3. **Sheets tabs**: `ASINs` is the master list, `Q{N}-{ASIN}` are quarterly trackers.
