# ADR-0004 · Serving protocol: REST/JSON now, gRPC as the documented production path

**Status:** Accepted · **Date:** 2026-09-24

## Context

The original brief specifies a gRPC serving API:

```proto
rpc GetFeatures(entity_id, feature_names[]) returns FeatureVector
```

gRPC is genuinely the right choice for a high-QPS, low-latency serving path: HTTP/2
multiplexing, binary Protobuf (smaller and faster than JSON), streaming, and generated
strongly-typed clients. So why does this repo expose REST/JSON?

## Decision

Implement the serving **business logic** (`serving.Service`) transport-agnostically, and
put REST/JSON in front of it as the concrete transport now. Document gRPC as the drop-in
production transport rather than generating Protobuf stubs in this environment.

Reasons:

1. **No codegen toolchain dependency.** gRPC requires `protoc` + plugins and a build step.
   Keeping the repo buildable with a bare `go build` (no external tools) matches the
   project's "runs anywhere" principle (ADR-0002).
2. **The interesting logic is transport-independent.** Batched reads, freshness
   annotation, cache-hit metrics, and feature logging all live in `serving.Service`, which
   knows nothing about HTTP or gRPC. Adding a gRPC server later is a thin adapter that
   calls the same `GetFeatures` method — no business logic moves.
3. **Testability.** `httptest` gives fast, dependency-free handler tests.

## What a gRPC adapter would look like

```proto
service FeatureService {
  rpc GetFeatures(GetRequest) returns (GetResponse);
}
message GetRequest { string entity = 1; repeated string entity_ids = 2; repeated string features = 3; }
```

The generated server's `GetFeatures` method would translate the Protobuf request into a
`serving.GetRequest`, call `svc.GetFeatures`, and translate the result back. The service,
stores, metrics, and logger are all reused unchanged. That is precisely the payoff of
keeping transport out of the core.

## Consequences

- **Good:** zero build tooling, fast tests, and the migration to gRPC is additive.
- **Cost:** JSON is heavier on the wire than Protobuf. For the demo and learning goals this
  is irrelevant; for a real high-QPS deployment you'd add the gRPC adapter, and the seam is
  ready for it.
