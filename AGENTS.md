# Agent Guidelines

## Repository Structure

This is a **mono-repo** containing all NRG (Narrative Risk Guard) components.

## Version Control Guidelines

### Rebase-based Workflow

This repository uses a **rebase-based workflow** (no merge commits):

1. **Always rebase, never merge**
   ```bash
   git pull --rebase origin main
   ```

2. **Keep commits atomic and meaningful**
   - Each commit should represent a single logical change
   - Write clear commit messages: `<type>: <description>`
   - Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

3. **Before pushing**
   ```bash
   git fetch origin
   git rebase origin/main
   ```

4. **Feature branches**
   ```bash
   git checkout -b feature/<name>
   # ... work ...
   git rebase -i origin/main  # squash/fixup as needed
   git push origin feature/<name>
   ```

5. **Pull Request rules**
   - Rebase onto latest `main` before merging
   - Use "Rebase and merge" or "Squash and merge" (never "Create a merge commit")
   - Delete branch after merge

### Commit Message Format

```
<type>: <short description>

[optional body]

[optional footer]
```

Examples:
- `feat: add Schwab API position fetching`
- `fix: correct utilization calculation for short options`
- `refactor: extract thesis mapping to separate module`
- `test: add risk engine mode transition tests`

---

## Testing Requirements

Always create useful unit tests when writing or modifying code:

- Every new module should have corresponding test files in `tests/`
- Test the core logic, edge cases, and error handling
- Use pytest as the testing framework
- Aim for meaningful coverage of business logic, not just line coverage

## Test Structure

```
tests/
├── test_risk_engine.py
├── test_storage.py
├── test_connectors/
│   ├── test_schwab.py
│   └── test_fidelity_csv.py
└── test_sheets_writer.py
```

## What to Test

1. **Risk Engine**: Mode computation, thesis utilization calculations, reduction amounts
2. **Connectors**: Data parsing, normalization, error handling
3. **Storage**: Database operations, data persistence
4. **Sheets Writer**: Data formatting, schema compliance

## Running Tests

```bash
pytest tests/ -v
pytest tests/ --cov=src --cov-report=term-missing
```
