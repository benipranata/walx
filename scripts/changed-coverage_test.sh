#!/bin/sh
# Smoke test for changed-coverage.sh: builds a throwaway repo and checks the table.
set -eu
script=$(cd "$(dirname "$0")" && pwd)/changed-coverage.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"
git init -q -b main
git config user.email t@t && git config user.name t
printf 'module example.com/m\n\ngo 1.21\n' > go.mod
mkdir pkg
printf 'package pkg\n' > pkg/a.go
git add . && git commit -qm base
git checkout -qb feat
printf 'package pkg\n\nfunc F() {}\n' > pkg/a.go
printf 'package pkg\n' > pkg/empty.go
printf 'package pkg\n' > pkg/b_test.go
mkdir pkg/mock
printf 'package mock\n' > pkg/mock/mock_x.go
printf 'package pkg\n' > "pkg/sp ace.go"
git add . && git commit -qm change
cat > cov.out <<'COV'
mode: atomic
example.com/m/pkg/a.go:3.10,3.12 2 1
example.com/m/pkg/a.go:4.1,5.2 2 0
example.com/m/pkg/empty.go:1.1,1.2 0 0
COV
out=$(sh "$script" cov.out main)
echo "$out"
echo "$out" | grep -qF '| `pkg/a.go` | 50.0% (2/4) |'
echo "$out" | grep -qF '| `pkg/empty.go` | no statements |'
echo "$out" | grep -qF '| `pkg/sp ace.go` | not in profile |'
! echo "$out" | grep -q b_test
! echo "$out" | grep -q /mock/
git checkout -q main
[ "$(sh "$script" cov.out main)" = "No non-test Go files changed." ]
echo ok
