# Contributing

Thanks for helping. Dispatch exists to publish one number honestly, so the bar
for a change is: does it make the number more trustworthy, or the system it
measures better, without quietly making the measurement kinder?

## Good contributions

- Bug fixes in the hub, the coalescing buffer or the websocket handling.
- A new `Feed` implementation (`internal/feed`). The interface is the point:
  energy prices, transit positions and odds are the same system with a slower
  clock.
- Benchmark results from other hardware, especially across a real network.
  Include how you measured, and which of the three clocks you are reporting.
- Anything in `dispatch-spec.md` §5, "what would make this dishonest", that is
  true of the code today.

Open an issue before a larger change, such as a different wire format or a
second transport, so we can agree it belongs here rather than in your fork.

## Making a change

```bash
gofmt -l .          # must print nothing
go vet ./...
go test -race ./...
```

CI runs exactly those. If a change moves the published numbers, update the
table in `README.md` in the same pull request and say what hardware produced
them, because a number without its conditions is not a number.
