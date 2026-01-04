# Agent Guidelines

## Repository Structure

This is a **mono-repo** containing all NRG (Narrative Risk Guard) components.

## Version Control Guidelines

### Rebase-based Workflow (Solo Contributor)

This repository uses a **rebase-based workflow** with direct pushes to `main` (no PRs required for sole contributor):

```bash
# Standard workflow
git add .
git commit -m "feat: description"
git push origin main

# If remote has changes
git pull --rebase origin main
git push origin main
```

### Commit Message Format

```
<type>: <short description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Examples:
- `feat: add Schwab API position fetching`
- `fix: correct utilization calculation for short options`
- `test: add risk engine mode transition tests`

---

## Testing Requirements

**Tests are mandatory.** All code changes must include appropriate tests.

### Coverage Threshold (STRICT)

**Minimum global coverage: 90%**

Before committing, **always check coverage in real-time**:

```bash
pytest tests/ --cov=src --cov-report=term-missing --cov-fail-under=90
```

### Coverage Rules

1. **Check coverage before every commit** - Run pytest with `--cov` flag
2. **If coverage is below 90% → CREATE MORE TESTS before committing**
3. **All tests must pass** - Zero tolerance for failing tests

### After Every Material Code Change

**MANDATORY**: After any material code change (bug fix, new feature, refactor), you MUST:

1. **Run all tests**:
   ```bash
   pytest tests/ -v
   ```

2. **Fix any failing tests** before committing

3. **Add new tests** if the change adds new functionality or edge cases

4. **Check coverage** hasn't dropped below 90%:
   ```bash
   pytest tests/ --cov=src --cov-report=term-missing
   ```

**DO NOT commit code that breaks existing tests.**

### Test Structure

```
tests/
├── conftest.py              # Shared fixtures
├── test_risk_engine.py      # Core risk logic tests
├── test_storage.py          # Database operation tests
├── test_sheets_writer.py    # Google Sheets output tests
└── test_connectors/
    ├── test_schwab.py       # Schwab API connector tests
    └── test_fidelity_csv.py # Fidelity CSV parser tests
```

### What Must Be Tested

| Module | Required Test Coverage |
|--------|----------------------|
| **Risk Engine** | Mode transitions (NORMAL/HALF/MIN), utilization calculations, reduction amounts, edge cases (zero equity, negative values) |
| **Connectors** | Data parsing, field normalization, malformed input handling, connection failures |
| **Storage** | CRUD operations, data integrity, query correctness |
| **Sheets Writer** | Schema compliance, data formatting, idempotency |

### Testing Best Practices

1. **Test the formulas** - Verify risk calculations match spec exactly:
   ```python
   # WorstLoss = MV * stress_pct
   # Budget$ = Equity * budget_pct * risk_scale
   # Utilization = WorstLoss / Budget$
   ```

2. **Test edge cases**:
   - Zero or negative equity
   - Empty positions
   - Missing thesis mappings
   - Broker connection failures
   - Mode boundary conditions (exactly at -12%, -24%)

3. **Test state transitions**:
   - Mode changes trigger correctly
   - Historical peak updates properly
   - Utilization breaches generate correct actions

4. **Use fixtures** - Define reusable test data in `conftest.py`

5. **Mock external services** - Never call real APIs in tests:
   - Mock Schwab API responses
   - Use sample CSV files for Fidelity
   - Mock Google Sheets API

### Running Specific Tests

```bash
# Run single test file
pytest tests/test_risk_engine.py -v

# Run specific test
pytest tests/test_risk_engine.py::test_mode_transitions -v

# Run tests matching pattern
pytest -k "utilization" -v

# Stop on first failure
pytest -x

# Show print statements
pytest -s
```
