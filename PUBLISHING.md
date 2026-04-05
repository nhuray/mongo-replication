# Publishing to PyPI

This guide explains how to publish the mongo-replication package to PyPI.

## Prerequisites

1. **PyPI Account**: Create accounts on:
   - [PyPI](https://pypi.org/account/register/) (production)
   - [TestPyPI](https://test.pypi.org/account/register/) (testing)

2. **API Tokens**: Generate API tokens from your PyPI account settings:
   - PyPI: Account Settings → API tokens → Add API token
   - TestPyPI: Same process on test.pypi.org

3. **Install Build Tools**:
   ```bash
   uv pip install build twine
   ```

## Publishing Steps

### 1. Test Locally

Ensure the package builds and installs correctly:

```bash
# Build the package
uv run python -m build

# This creates:
# - dist/mongo-replication-0.1.0.tar.gz
# - dist/mongo_replication-0.1.0-py3-none-any.whl

# Test installation locally
uv pip install dist/mongo_replication-0.1.0-py3-none-any.whl
```

### 2. Publish to TestPyPI (Recommended First)

Test the publishing process on TestPyPI:

```bash
# Upload to TestPyPI
uv run twine upload --repository testpypi dist/*

# When prompted:
# Username: __token__
# Password: <your-testpypi-api-token>
```

Test installation from TestPyPI:

```bash
pip install --index-url https://test.pypi.org/simple/ mongo-replication
```

### 3. Publish to PyPI (Production)

Once tested, publish to production PyPI:

```bash
# Upload to PyPI
uv run twine upload dist/*

# When prompted:
# Username: __token__
# Password: <your-pypi-api-token>
```

### 4. Verify Publication

Check your package on PyPI:
- https://pypi.org/project/mongo-replication/

Install and test:

```bash
pip install mongo-replication
mongo-replication --help
```

## Using GitHub Actions (Automated)

Create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      
      - name: Install uv
        run: pip install uv
      
      - name: Build package
        run: uv run python -m build
      
      - name: Publish to PyPI
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
        run: uv run twine upload dist/*
```

Add your PyPI API token to GitHub Secrets:
1. Go to repository Settings → Secrets and variables → Actions
2. Add secret: `PYPI_API_TOKEN` with your token value

## Version Management

Update version in `pyproject.toml`:

```toml
[project]
name = "mongo-replication"
version = "0.1.1"  # Update this
```

Or use a version management tool:

```bash
# Install bump2version
uv pip install bump2version

# Bump version
bump2version patch  # 0.1.0 → 0.1.1
bump2version minor  # 0.1.0 → 0.2.0
bump2version major  # 0.1.0 → 1.0.0
```

## Release Checklist

Before each release:

- [ ] Update version in `pyproject.toml`
- [ ] Update `CHANGELOG.md` with changes
- [ ] Run tests: `pytest`
- [ ] Build locally: `uv run python -m build`
- [ ] Test installation locally
- [ ] Test on TestPyPI
- [ ] Create git tag: `git tag v0.1.0`
- [ ] Push tag: `git push origin v0.1.0`
- [ ] Publish to PyPI
- [ ] Create GitHub Release with notes

## Troubleshooting

### "Package already exists"
- You cannot re-upload the same version
- Increment version number in `pyproject.toml`

### "Invalid credentials"
- Ensure you're using `__token__` as username
- Verify your API token is correct
- Check if using TestPyPI vs PyPI token

### Build fails
- Check `pyproject.toml` syntax
- Ensure all required files are included
- Verify Python version compatibility

### Import errors after install
- Check package structure in `dist/*.whl`
- Verify `[tool.hatch.build.targets.wheel]` configuration
- Ensure all imports use correct package name

## Resources

- [Python Packaging Guide](https://packaging.python.org/)
- [PyPI Help](https://pypi.org/help/)
- [Twine Documentation](https://twine.readthedocs.io/)
