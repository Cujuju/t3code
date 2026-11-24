import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import {
  HttpBody,
  HttpClient,
  HttpClientError,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/http";
import { OtlpExporter, OtlpSerialization, OtlpTracer } from "effect/observability";
import { describe } from "vite-plus/test";

import { otlpExportAttemptFailuresTotal } from "./Metrics.ts";
import * as OtlpExportFailures from "./OtlpExportFailures.ts";

const failures = (reason: string) =>
  Metric.value(
    Metric.withAttributes(otlpExportAttemptFailuresTotal, { signal: "logs", reason }),
  ).pipe(Effect.map((state) => state.count));

describe("OtlpExportFailures", () => {
  it.effect("counts each failed attempt by reason and leaves successes alone", () => {
    const outcomes: Array<number | "transport"> = [503, "transport", 200];
    const client = HttpClient.make((request) => {
      const outcome = outcomes.shift() ?? 200;
      return outcome === "transport"
        ? Effect.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({ request }),
            }),
          )
        : Effect.succeed(
            HttpClientResponse.fromWeb(request, new Response(null, { status: outcome })),
          );
    });
    const serialization = OtlpSerialization.OtlpSerialization.of({
      traces: () => HttpBody.empty,
      metrics: () => HttpBody.empty,
      logs: () => {
        throw new Error("unencodable attribute");
      },
    });
    const layer = OtlpExportFailures.layer("logs").pipe(
      Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
      Layer.provide(Layer.succeed(OtlpSerialization.OtlpSerialization, serialization)),
    );

    return Effect.gen(function* () {
      const counted = yield* HttpClient.HttpClient;
      const encode = yield* OtlpSerialization.OtlpSerialization;
      const request = HttpClientRequest.post("http://localhost:4318/v1/logs");
      yield* counted.execute(request);
      yield* Effect.flip(counted.execute(request));
      yield* counted.execute(request);
      assert.throws(() => encode.logs({ resourceLogs: [] }), "unencodable attribute");

      const transport = yield* Metric.value(
        Metric.withAttributes(otlpExportAttemptFailuresTotal, {
          signal: "logs",
          reason: "transport",
        }),
      );
      // OTLP marks the sum monotonic only for an incremental counter.
      assert.isTrue(transport.incremental);
      assert.deepStrictEqual(
        [
          yield* failures("status_503"),
          yield* failures("transport"),
          yield* failures("encode"),
          yield* failures("status_200"),
        ],
        [1, 1, 1, 0],
      );
    }).pipe(Effect.provide(layer), Effect.provideService(Metric.MetricRegistry, new Map()));
  });
});

const makeTracer = (client: HttpClient.HttpClient) =>
  OtlpTracer.make({
    url: "http://localhost:4318/v1/traces",
    resource: { serviceName: "t3-test" },
    exportInterval: "1 second",
    maxBatchSize: 2,
    maxBufferedSpans: 2,
    exportTimeout: "10 seconds",
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        OtlpExporter.layerFlusher,
        OtlpSerialization.layerJson,
        Layer.succeed(HttpClient.HttpClient, client),
      ),
    ),
  );

const success = (request: HttpClientRequest.HttpClientRequest) =>
  HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }));
const emit = (name: string) => Effect.useSpan(name, Effect.succeed);

describe("bounded OTLP trace export", () => {
  it.effect("serializes exports and sends only the admitted spans", () =>
    Effect.gen(function* () {
      const reply = yield* Deferred.make<void>();
      const batches: string[][] = [];
      const tracer = yield* makeTracer(
        HttpClient.make((request) =>
          Effect.gen(function* () {
            assert(request.body._tag === "Uint8Array");
            const payload = JSON.parse(
              new TextDecoder().decode(request.body.body),
            ) as OtlpTracer.TraceData;
            batches.push(
              payload.resourceSpans.flatMap((resource) =>
                resource.scopeSpans.flatMap((scope) => scope.spans.map((span) => span.name)),
              ),
            );
            if (batches.length === 1) yield* Deferred.await(reply);
            return success(request);
          }),
        ),
      );
      yield* Effect.addFinalizer(() => Deferred.succeed(reply, undefined));

      yield* emit("first").pipe(Effect.withTracer(tracer));
      yield* TestClock.adjust("1 second");
      assert.deepEqual(batches, [["first"]]);
      const spans = yield* Effect.forEach(["second", "third", "overflow"], emit).pipe(
        Effect.withTracer(tracer),
      );
      yield* TestClock.adjust("1 second");
      assert.deepEqual(batches, [["first"]]);
      assert.isTrue(spans.every((span) => span.status._tag === "Ended"));

      yield* Deferred.succeed(reply, undefined);
      yield* TestClock.adjust("2 seconds");
      assert.deepEqual(batches, [["first"], ["second", "third"]]);
    }),
  );

  it.effect("backs off timed-out requests until the cooldown ends", () =>
    Effect.gen(function* () {
      const aborted = yield* Deferred.make<void>();
      const cleanup = yield* Deferred.make<void>();
      let requests = 0;
      const tracer = yield* makeTracer(
        HttpClient.make((request) =>
          Effect.suspend(() => {
            requests++;
            return requests === 1
              ? Deferred.await(cleanup).pipe(
                  Effect.onInterrupt(() => Deferred.succeed(aborted, undefined)),
                  Effect.as(success(request)),
                )
              : Effect.succeed(success(request));
          }),
        ),
      );
      yield* Effect.addFinalizer(() => Deferred.succeed(cleanup, undefined));
      yield* emit("blocked").pipe(Effect.withTracer(tracer));
      yield* TestClock.adjust("1 second");
      assert.strictEqual(requests, 1);
      yield* TestClock.adjust("10 seconds");
      assert.isTrue(yield* Deferred.isDone(aborted));

      yield* emit("during-cooldown").pipe(Effect.withTracer(tracer));
      yield* TestClock.adjust("59 seconds");
      assert.strictEqual(requests, 1);
      yield* TestClock.adjust("1 second");
      yield* emit("recovered").pipe(Effect.withTracer(tracer));
      yield* TestClock.adjust("1 second");
      assert.strictEqual(requests, 2);
    }),
  );

  it.effect("lets the in-flight export finish before flushing pending spans at shutdown", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const reply = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(reply, undefined).pipe(Effect.andThen(Scope.close(scope, Exit.void))),
      );
      let requests = 0;
      let interrupted = false;
      const tracer = yield* makeTracer(
        HttpClient.make((request) =>
          Effect.gen(function* () {
            if (++requests === 1) {
              yield* Deferred.await(reply).pipe(
                Effect.onInterrupt(() =>
                  Effect.sync(() => {
                    interrupted = true;
                  }),
                ),
              );
            }
            return success(request);
          }),
        ),
      ).pipe(Scope.provide(scope));
      yield* emit("first").pipe(Effect.withTracer(tracer));
      yield* TestClock.adjust("1 second");
      assert.strictEqual(requests, 1);

      yield* emit("pending").pipe(Effect.withTracer(tracer));
      const shutdown = yield* Scope.close(scope, Exit.void).pipe(Effect.forkScoped);
      yield* TestClock.adjust("1 millis");
      assert.strictEqual(requests, 1);
      assert.isFalse(interrupted);
      assert.isUndefined(shutdown.pollUnsafe());
      yield* Deferred.succeed(reply, undefined);
      yield* Fiber.join(shutdown);
      assert.strictEqual(requests, 2);
      assert.isFalse(interrupted);
    }),
  );
});
