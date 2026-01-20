#!/bin/bash
# EventGate Local CI Quality Gates

set -eo pipefail

echo "=== EventGate Local CI ==="
echo ""

echo "[1/4] Black (formatting)..."
black --check $(git ls-files '*.py') || { echo "Run: black \$(git ls-files '*.py')"; exit 1; }
echo "✓ Black passed"
echo ""

echo "[2/4] Pylint (linting)..."
pylint --fail-under=9.5 $(git ls-files '*.py') || exit 1
echo "✓ Pylint passed (≥9.5)"
echo ""

echo "[3/4] mypy (type checking)..."
mypy src/ || exit 1
echo "✓ mypy passed"
echo ""

echo "[4/4] pytest (tests + coverage)..."
pytest --cov=src --cov-fail-under=80 -q tests/ || exit 1
echo "✓ pytest passed (≥80% coverage)"
echo ""

echo "=== All quality gates passed ✓ ==="
