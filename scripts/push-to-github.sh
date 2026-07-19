#!/usr/bin/env bash
# Initialize git and push this project to a PRIVATE GitHub repo.
# Run from the repo root on YOUR machine (Mac/Linux/WSL):  bash scripts/push-to-github.sh
set -euo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"

# Start clean (removes any broken .git left by the sandbox).
rm -rf .git
git init -q
git add -A

if ! git commit -q -m "Initial commit: otel-data-staleness"; then
  echo "!! Set your git identity first, then re-run:"
  echo "   git config --global user.email \"you@example.com\""
  echo "   git config --global user.name  \"Your Name\""
  exit 1
fi
echo "Committed $(git ls-files | wc -l) files."

if command -v gh >/dev/null 2>&1; then
  # GitHub CLI present: create the private repo and push in one step.
  gh repo create otel-data-staleness --private --source=. --remote=origin --push
  echo "Done: private repo created and pushed via gh."
else
  git branch -M main
  cat <<'NEXT'

Next steps (no GitHub CLI found):
  1. Create an EMPTY private repo at https://github.com/new
     (name it otel-data-staleness, do NOT add a README/.gitignore).
  2. Then run:
       git remote add origin https://github.com/<your-username>/otel-data-staleness.git
       git push -u origin main
     (For a private repo, use a GitHub Personal Access Token as the password.)
NEXT
fi
