# Distributed Dead Man's Switch (CMPE 273)

A heartbeat-and-failure-detection system written in Go. Implements push and pull heartbeat transports and Fixed Window + Phi Accrual failure detectors. See `docs/superpowers/specs/` for design and `paper/` for the research write-up.

## Build
    make build         # produces bin/monitor and bin/worker

## Test
    make test

## Demo
    make run-demo
