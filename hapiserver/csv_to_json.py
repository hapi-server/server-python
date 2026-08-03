#!/usr/bin/env python3

import argparse
import csv
import json
import math
import sys
from typing import List, Optional, Sequence


class CsvToJsonError(ValueError):
  pass


def _coerce_token(token: str):
  token = token.strip()

  if token == "":
    return ""

  lower = token.lower()
  if lower == "nan":
    return math.nan
  if lower == "inf":
    return math.inf
  if lower == "-inf":
    return -math.inf

  try:
    if any(ch in lower for ch in [".", "e"]):
      return float(token)
    return int(token)
  except ValueError:
    return token


def _parse_size(size_text: Optional[str]) -> Optional[List[int]]:
  if size_text is None or size_text.strip() == "":
    return None

  parts = [p.strip() for p in size_text.split(",") if p.strip() != ""]
  if not parts:
    return None

  size = []
  for part in parts:
    try:
      value = int(part)
    except ValueError as exc:
      raise CsvToJsonError(f"Invalid size entry '{part}'.") from exc
    if value <= 0:
      raise CsvToJsonError("All size dimensions must be positive integers.")
    size.append(value)

  return size


def _product(values: Sequence[int]) -> int:
  p = 1
  for value in values:
    p *= value
  return p


def _reshape(flat_values: Sequence[object], size: Sequence[int]):
  if len(size) == 1:
    return list(flat_values)

  first = size[0]
  rest = size[1:]
  chunk_len = _product(rest)
  shaped = []
  for idx in range(first):
    start = idx * chunk_len
    stop = start + chunk_len
    shaped.append(_reshape(flat_values[start:stop], rest))
  return shaped


def convert_row(csv_row: Sequence[str], size: Optional[Sequence[int]] = None):
  if len(csv_row) < 2:
    raise CsvToJsonError("CSV row must contain at least time + one value.")

  timestamp = csv_row[0].strip()
  values = [_coerce_token(tok) for tok in csv_row[1:]]

  if size is None:
    if len(values) != 1:
      raise CsvToJsonError(
        "No size was provided, so exactly one scalar value is expected."
      )
    if isinstance(values[0], list):
      raise CsvToJsonError("No size was provided; array value is not allowed.")
    payload = values[0]
  else:
    expected = _product(size)
    if len(values) != expected:
      raise CsvToJsonError(
        f"size={list(size)} expects {expected} value(s), got {len(values)}."
      )
    payload = _reshape(values, size)

  return [timestamp, payload]


def convert_line(csv_line: str, size: Optional[Sequence[int]] = None):
  try:
    parsed = next(csv.reader([csv_line], skipinitialspace=True))
  except Exception as exc:
    raise CsvToJsonError(f"Invalid CSV line: {csv_line}") from exc
  return convert_row(parsed, size=size)


def _warn_if_discouraged_size(size: Optional[Sequence[int]]):
  if size is None:
    return
  if any(dim == 1 for dim in size):
    print(
      "Warning: size includes dimension 1. This is allowed but discouraged because "
      "clients may interpret shape ambiguously.",
      file=sys.stderr,
    )


def _run_examples():
  examples = [
    ("2010-001T12:01:00Z, 1.0", None),
    ("2010-001T12:01:00Z, 1.0", [1]),
    ("2010-001T12:01:00Z, 1", [1, 1]),
    ("2010-001T12:01:00Z, 1.0, 2.0", [1, 2]),
    ("2010-001T12:01:00Z, 1.0, 2.0", [1, 2, 1]),
  ]

  for line, size in examples:
    try:
      print(json.dumps(convert_line(line, size=size)))
    except CsvToJsonError as exc:
      print(f"Error: {exc}")


def main(argv: Optional[Sequence[str]] = None) -> int:
  parser = argparse.ArgumentParser(
    description=(
      "Convert a single HAPI-like CSV row (time + one parameter payload) to JSON, "
      "using optional info.size."
    )
  )
  parser.add_argument(
    "--line",
    default=None,
    help="CSV line to convert. If omitted, one line is read from stdin.",
  )
  parser.add_argument(
    "--size",
    default=None,
    help="Comma-separated shape, e.g. '1', '1,2', '1,2,1'.",
  )
  parser.add_argument(
    "--examples",
    action="store_true",
    help="Run built-in examples.",
  )

  args = parser.parse_args(argv)

  if args.examples:
    _run_examples()
    return 0

  size = _parse_size(args.size)
  _warn_if_discouraged_size(size)

  line = args.line
  if line is None:
    line = sys.stdin.readline().rstrip("\n")

  try:
    converted = convert_line(line, size=size)
  except CsvToJsonError as exc:
    print(f"Error: {exc}", file=sys.stderr)
    return 2

  print(json.dumps(converted))
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
