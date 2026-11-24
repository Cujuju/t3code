// @effect-diagnostics nodeBuiltinImport:off -- Drive the native callback without a Windows host.
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { vi } from "vite-plus/test";

import { watchDirectoryPath } from "./watchDirectoryReplacements.ts";

const nativeFs: typeof NodeFS = NodeModule.createRequire(import.meta.url)("node:fs");

class TestWatcher extends NodeEvents.EventEmitter implements NodeFS.FSWatcher {
  close = vi.fn();
  ref() {
    return this;
  }
  unref() {
    return this;
  }
}

const stubNativeWatch = Effect.gen(function* () {
  const ready = yield* Deferred.make<NodeFS.WatchListener<string>>();
  const watcher = new TestWatcher();
  yield* Effect.acquireRelease(
    Effect.sync(() => {
      const mock = vi
        .spyOn(nativeFs, "watch")
        .mockImplementation((_directory: NodeFS.PathLike, options: unknown, listener?: unknown) => {
          const callback = typeof options === "function" ? options : listener;
          if (typeof callback !== "function") throw new Error("Missing watch callback");
          queueMicrotask(() =>
            Deferred.doneUnsafe(ready, Effect.succeed(callback as NodeFS.WatchListener<string>)),
          );
          return watcher;
        });
      NodeModule.syncBuiltinESMExports();
      return mock;
    }),
    (mock) =>
      Effect.sync(() => {
        mock.mockRestore();
        NodeModule.syncBuiltinESMExports();
      }),
  );
  return {
    watcher,
    ready: Deferred.await(ready),
    windows: (notify: () => void) => {
      const platform = Object.getOwnPropertyDescriptor(process, "platform");
      Object.defineProperty(process, "platform", { value: "win32" });
      try {
        notify();
      } finally {
        if (platform) Object.defineProperty(process, "platform", platform);
      }
    },
  };
});

it.live("ends a directory watch synchronously on a Windows self-rename", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped();
    const { watcher, ready, windows } = yield* stubNativeWatch;
    const events = yield* fs.watch(directory).pipe(Stream.runCollect, Effect.forkScoped);
    const notify = yield* ready;
    const info = NodeFS.statSync(directory);
    const pending: Array<() => void> = [];
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const mock = vi.spyOn(nativeFs, "stat").mockImplementation(((
          _path: NodeFS.PathLike,
          callback: (error: NodeJS.ErrnoException | null, stats: NodeFS.Stats) => void,
        ) => {
          pending.push(() => callback(null, info));
        }) as typeof NodeFS.stat);
        NodeModule.syncBuiltinESMExports();
        return mock;
      }),
      (mock) =>
        Effect.sync(() => {
          mock.mockRestore();
          NodeModule.syncBuiltinESMExports();
        }),
    );
    const self = "\\\\?\\UNC\\server\\share\\";
    windows(() => {
      notify("rename", "share");
      assert.strictEqual(watcher.close.mock.calls.length, 0);
      notify("rename", self);
      assert.strictEqual(watcher.close.mock.calls.length, 1);
      assert.lengthOf(pending, 1);
      notify("rename", self);
      notify("change", "HEAD");
    });
    pending[0]?.();
    assert.deepEqual(yield* Fiber.join(events), [{ _tag: "Remove", path: self }]);
    assert.strictEqual(watcher.close.mock.calls.length, 1);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["file system", "replacement"] as const)(
  "closes the %s native watch synchronously on error",
  (kind) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped();
      const { watcher, ready } = yield* stubNativeWatch;
      const drain =
        kind === "file system"
          ? Stream.runDrain(fs.watch(directory))
          : Stream.runDrain(
              watchDirectoryPath(
                path.join(directory, "themes"),
                Stream.empty,
                Effect.succeed(false),
              ),
            );
      yield* drain.pipe(Effect.exit, Effect.forkScoped);
      yield* ready;
      watcher.emit("error", new Error("fixture"));
      assert.strictEqual(watcher.close.mock.calls.length, 1);
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("closes a replacement watch on the parent directory's Windows self-rename", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped();
    const { watcher, ready } = yield* stubNativeWatch;
    yield* watchDirectoryPath(
      path.join(directory, "themes"),
      Stream.never,
      Effect.succeed(false),
    ).pipe(Stream.runDrain, Effect.forkScoped);
    const notify = yield* ready;
    notify("rename", "themes");
    assert.strictEqual(watcher.close.mock.calls.length, 0);
    notify("rename", "E:\\");
    notify("rename", "E:\\");
    assert.strictEqual(watcher.close.mock.calls.length, 1);
  }).pipe(Effect.provide(NodeServices.layer), Effect.provideService(HostProcess.Platform, "win32")),
);

it.live.skipIf(!symlinksSupported)(
  "backs off stopped watches for an unchanged linked directory",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const base = yield* fs.makeTempDirectoryScoped();
      const entry = path.join(base, "linked");
      const first = path.join(base, "first", "themes");
      const second = path.join(base, "second", "themes");
      yield* fs.makeDirectory(first, { recursive: true });
      yield* fs.makeDirectory(second, { recursive: true });
      yield* fs.symlink(first, entry);
      const firstIdentity = yield* fs.stat(first);
      const secondIdentity = yield* fs.stat(second);
      let identity = firstIdentity;
      const events = yield* Queue.unbounded<"probe" | "refresh">();

      // An ended child emits a final refresh, as it does when registration fails.
      yield* watchDirectoryPath(entry, Stream.succeed(undefined), Effect.succeed(false)).pipe(
        Stream.runForEach(() => Queue.offer(events, "refresh")),
        Effect.provideService(
          FileSystem.FileSystem,
          FileSystem.FileSystem.of({
            ...fs,
            stat: (file) =>
              file === entry
                ? Queue.offer(events, "probe").pipe(Effect.map(() => identity))
                : fs.stat(file),
          }),
        ),
        Effect.forkScoped,
      );
      assert.deepEqual(
        yield* Queue.takeN(events, 10),
        // Each acquire stats too: the first, a retry after 1 tick, then after 2 more.
        [
          "probe",
          "probe",
          "refresh",
          "probe",
          "probe",
          "refresh",
          "probe",
          "probe",
          "probe",
          "refresh",
        ],
      );

      identity = secondIdentity;
      assert.deepEqual(yield* Queue.takeN(events, 3), ["probe", "probe", "refresh"]);
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.live.skipIf(!symlinksSupported)(
  "rebuilds when a probe sees a tree other than the one the watches were acquired on",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const base = yield* fs.makeTempDirectoryScoped();
      const entry = path.join(base, "linked");
      const first = path.join(base, "first");
      const second = path.join(base, "second");
      yield* fs.makeDirectory(first);
      yield* fs.makeDirectory(second);
      yield* fs.symlink(first, entry);
      const a = yield* fs.stat(first);
      const b = yield* fs.stat(second);
      // Probe A, acquire A, probe B, swap back and acquire A, then B returns for the next probe.
      const identities = [a, a, b, a, b, b];
      const refreshes = yield* Queue.unbounded<void>();

      yield* watchDirectoryPath(
        entry,
        Stream.concat(Stream.succeed(undefined), Stream.never),
        Effect.succeed(false),
      ).pipe(
        Stream.runForEach(() => Queue.offer(refreshes, undefined)),
        Effect.provideService(
          FileSystem.FileSystem,
          FileSystem.FileSystem.of({
            ...fs,
            stat: (file) =>
              file === entry ? Effect.sync(() => identities.shift() ?? b) : fs.stat(file),
          }),
        ),
        Effect.forkScoped,
      );
      yield* Queue.takeN(refreshes, 3);
      assert.deepEqual(identities, []);
    }).pipe(Effect.provide(NodeServices.layer)),
);
