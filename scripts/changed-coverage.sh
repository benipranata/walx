#!/bin/sh
# Usage: changed-coverage.sh <coverage.out> <base-ref>
# Prints a markdown table of statement coverage for the Go files changed
# between <base-ref> and HEAD (non-test files only; tests have no statements; generated mocks excluded).
set -eu
if [ $# -ne 2 ]; then
  echo "usage: $0 <coverage.out> <base-ref>" >&2
  exit 2
fi
profile=$1
base=$2
module=$(go list -m)

files=$(git diff --name-only --diff-filter=d "$base...HEAD" -- '*.go' | grep -v -e '_test\.go$' -e '/mock/' || true)
if [ -z "$files" ]; then
  echo "No non-test Go files changed."
  exit 0
fi

echo "| File | Coverage |"
echo "| --- | --- |"
printf '%s\n' "$files" | while IFS= read -r f; do
  awk -v name="$module/$f" -v short="$f" '
    index($0, name ":") == 1 { seen = 1; n += $2; if ($3 > 0) c += $2 }
    END {
      if (!seen) printf "| `%s` | not in profile |\n", short
      else if (n == 0) printf "| `%s` | no statements |\n", short
      else printf "| `%s` | %.1f%% (%d/%d) |\n", short, 100 * c / n, c, n
    }
  ' "$profile"
done
