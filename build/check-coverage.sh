#!/usr/bin/env bash
set -euo pipefail

profile="${1:-cover.out}"
floors_file="${2:-build/coverage-floors.txt}"

if [[ ! -f "${profile}" ]]; then
  echo "coverage profile not found: ${profile}" >&2
  exit 2
fi

if [[ ! -f "${floors_file}" ]]; then
  echo "coverage floors file not found: ${floors_file}" >&2
  exit 2
fi

module_path="$(go list -m)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

profile_mode="$(head -n 1 "${profile}")"
case "${profile_mode}" in
  mode:\ *) ;;
  *)
    echo "coverage profile has invalid mode line: ${profile}" >&2
    exit 2
    ;;
esac

failures=0
checked=0

printf 'Checking core package coverage floors from %s\n' "${floors_file}"

while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
  line="${raw_line%%#*}"
  read -r package floor extra <<< "${line}"

  if [[ -z "${package:-}" ]]; then
    continue
  fi

  if [[ -z "${floor:-}" || -n "${extra:-}" ]]; then
    echo "invalid floor entry: ${raw_line}" >&2
    exit 2
  fi

  full_package="${module_path}/${package}"
  package_profile="${tmp_dir}/${package//\//_}.out"

  # Slice the single CI coverage profile to this exact package, then let
  # go tool cover compute the package statement total. This avoids a second
  # test run and avoids accidentally counting nested subpackages.
  {
    printf '%s\n' "${profile_mode}"
    awk -v pkg="${full_package}" '
      index($1, pkg "/") == 1 {
        rest = substr($1, length(pkg) + 2)
        if (rest !~ /\//) {
          print
        }
      }
    ' "${profile}"
  } > "${package_profile}"

  if [[ "$(wc -l < "${package_profile}")" -le 1 ]]; then
    printf 'FAIL %s: no coverage data found in %s\n' "${package}" "${profile}" >&2
    failures=$((failures + 1))
    continue
  fi

  actual="$(go tool cover -func="${package_profile}" | awk '/^total:/ { pct=$3; sub(/%$/, "", pct); print pct }')"

  if [[ -z "${actual}" ]]; then
    printf 'FAIL %s: could not read package coverage\n' "${package}" >&2
    failures=$((failures + 1))
    continue
  fi

  if ! awk -v actual="${actual}" -v floor="${floor}" 'BEGIN { exit (actual + 0 >= floor + 0) ? 0 : 1 }'; then
    printf 'FAIL %s: %.1f%% < %.1f%%\n' "${package}" "${actual}" "${floor}" >&2
    failures=$((failures + 1))
  else
    printf 'PASS %s: %.1f%% >= %.1f%%\n' "${package}" "${actual}" "${floor}"
  fi

  checked=$((checked + 1))
done < "${floors_file}"

if [[ "${checked}" -eq 0 ]]; then
  echo "no coverage floors configured in ${floors_file}" >&2
  exit 2
fi

if [[ "${failures}" -ne 0 ]]; then
  printf 'Coverage floor check failed: %d package(s) below floor.\n' "${failures}" >&2
  exit 1
fi

printf 'Coverage floor check passed for %d core package(s).\n' "${checked}"
