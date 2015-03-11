#!/usr/bin/env bats

@test "patch binary is found in PATH" {
  run which patch
  [ "$status" -eq 0 ]
}
