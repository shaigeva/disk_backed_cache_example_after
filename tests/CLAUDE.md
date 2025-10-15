# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working on tests in this repository.

## Testing process

### **How to test each step**
After implementing a step, you MUST implement tests for it.
- Plan tests for the behavior in detail, follow all testing guidelines below.
- Implement the tests for this step.
- Consider if any tests for previous steps need to be updated due to changes in the code, for example if existing behaviors need to support new features.
- Follow the Testing & Validations Strategy for running validations and fixing issues.

## **Core Development Principles**
- **One Change = One Complete, testable Capability**
- **Validation-Driven Development** (every change must pass all validations)
- **Test-First Implementation** (behavior tests that target the API of the package are Priority 1)
- **No Layer-Based Changes**. Complete capabilities only - DO NOT implement multiple distinct capabilities in the same change. DO implement a capability and all its tests before continuing to the next capability.

### **Testing guidelines**
A test should almost always test a single fact about the behavior of the code. test_after_put_get_returns_the_object is a good example.

Cover ALL behaviors of a feature as much as possible.

Test scope: Test behavior == cohesive whole == complete story
for example, in order to test put(), you also need to test get().
The "truth" of how some component or full software is its external observable behavior. Tests should verify using that behavior is much as possible.
It's ok to test a small part of something larger - as long as that part is a "complete stroy" in itself.
Test behaviors instead of implementations (again, an implementation detail that is in itself a cohesive whole is fine to test).
If you created a test that uses implementation details such is private members - ask yourself if you can write the test
to verify the same behavior only using external API. If yes - re-write the test.

Tests must be isolated so they never interfere with each other.

Tests must use clear language: decisive, specific and explicit.

Avoid using mocks. Simulators for quick tests are fine (for example, it's ok to test using in-memory sqlite some of the time instead of disk based for performance).

Test a variety of inputs, including variations on input data, including edge cases.

Test file names should be significant, specific and descriptive of content.
GOOD: test_basic_put_and_get.py
BAD: test_step_03_basic_put_and_get.py
BAD: test_cache.py

### **Database Path Fixture (`db_path`)**

**ALWAYS use the `db_path` fixture for all tests that need a database path.**

The `db_path` fixture is defined in `tests/conftest.py` and provides configurable database paths:

```python
@pytest.fixture
def db_path(request: pytest.FixtureRequest) -> Generator[str, None, None]:
    """Provide database path based on --db-mode parameter.

    - In 'disk' mode (default): Creates a temporary file for each test
    - In 'memory' mode: Returns ':memory:' for in-memory database

    Each test gets its own isolated database.
    """
```

**Usage in tests:**
```python
def test_something(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=MyModel,
        # ... other parameters
    )
    # ... test code
```

**Running tests:**
- Default (disk mode): `pytest` or `pytest --db-mode=disk`
- Memory mode (faster): `pytest --db-mode=memory`

**Important notes:**
- The default mode is `disk` to ensure tests reflect real-world usage
- Each test gets an isolated database (no interference between tests)
- Tests requiring disk persistence should skip in memory mode:
  ```python
  def test_persistence(db_path: str, request: pytest.FixtureRequest) -> None:
      if request.config.getoption("--db-mode") == "memory":
          pytest.skip("Test requires disk persistence")
      # ... test code
  ```
