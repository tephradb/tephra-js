// Integration tests against a real tephra-server. They are gated so `npm test` (unit) stays
// dependency-free; run them with `npm run test:integration`.
//
// Point TEPHRA_SERVER_BIN at a prebuilt server binary, or set TEPHRA_REPO to a checkout of the Rust
// repo (default ../tephra) and the suite builds `tephra-server` with cargo. Without either, and with
// no cargo available, the suite skips itself.

import { type ChildProcess, execFile, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import {
  AppendCondition,
  Client,
  ErrorCode,
  Event,
  Query,
  QueryItem,
  ServerError,
  ZERO,
  isCaughtUp,
} from "../src/index.js";

const run = promisify(execFile);

const PORT = Number(process.env.TEPHRA_TEST_PORT ?? 19000);
const HOST = "127.0.0.1";

let server: ChildProcess | undefined;
let dataDir: string | undefined;
let serverBin: string | undefined;

async function resolveServerBinary(): Promise<string | undefined> {
  if (process.env.TEPHRA_SERVER_BIN && existsSync(process.env.TEPHRA_SERVER_BIN)) {
    return process.env.TEPHRA_SERVER_BIN;
  }
  const repo = process.env.TEPHRA_REPO ?? join(process.cwd(), "..", "tephra");
  if (!existsSync(repo)) {
    return undefined;
  }
  try {
    await run("cargo", ["build", "--release", "-p", "tephra-server"], {
      cwd: repo,
      timeout: 240_000,
    });
  } catch {
    return undefined;
  }
  const built = join(repo, "target", "release", "tephra-server");
  return existsSync(built) ? built : undefined;
}

async function waitForServer(client: () => Promise<Client>, attempts = 50): Promise<Client> {
  let lastError: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await client();
    } catch (err) {
      lastError = err;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  throw lastError;
}

beforeAll(async () => {
  serverBin = await resolveServerBinary();
  if (!serverBin) {
    return;
  }
  dataDir = await mkdtemp(join(tmpdir(), "tephra-it-"));
  server = spawn(serverBin, ["--bind", `${HOST}:${PORT}`, "--data-dir", dataDir], {
    stdio: "ignore",
  });
}, 300_000);

afterAll(async () => {
  server?.kill("SIGKILL");
  if (dataDir) {
    await rm(dataDir, { recursive: true, force: true });
  }
});

describe.runIf(process.env.TEPHRA_SERVER_BIN || existsSync(join(process.cwd(), "..", "tephra")))(
  "against a live tephra-server",
  () => {
    test("append, read, read_back, subscribe, stats, and a conflict", async () => {
      if (!serverBin) {
        return; // resolveServerBinary gave up (no cargo, no binary); nothing to test against.
      }
      const client = await waitForServer(() => Client.connect(`${HOST}:${PORT}`));
      try {
        // Append two events as one batch.
        const first = Event.create("CourseOpened", ["course:c1"], new TextEncoder().encode("{}"));
        const second = Event.create("Enrolled", ["course:c1", "student:s1"]);
        const result = await client.append([first, second]);
        expect(result.last - result.first).toBe(1n);

        // Read them back forward.
        const forward = await client.readAll(Query.all(), ZERO);
        expect(forward.events.length).toBeGreaterThanOrEqual(2);
        expect(forward.watermark).toBeGreaterThanOrEqual(result.last);
        const types = forward.events.map((event) => event.event.type);
        expect(types).toContain("CourseOpened");
        expect(types).toContain("Enrolled");

        // Read newest-first from the tip.
        const backward = await client.readAllBack(Query.all());
        const backwardPositions = backward.events.map((event) => event.position);
        const sortedDesc = [...backwardPositions].sort((a, b) => (a > b ? -1 : a < b ? 1 : 0));
        expect(backwardPositions).toEqual(sortedDesc);

        // Subscribe: catch up, then observe a caught-up marker.
        const seen: string[] = [];
        for await (const item of client.subscribe(Query.all(), ZERO)) {
          if (isCaughtUp(item)) {
            break;
          }
          seen.push(item.event.event.type);
        }
        expect(seen).toContain("CourseOpened");

        // Stats reflect the durable events.
        const stats = await client.stats();
        expect(stats.eventCount).toBeGreaterThanOrEqual(result.last);
        expect(stats.version.length).toBeGreaterThan(0);

        // A uniqueness guard: a second reservation for the same tag must conflict.
        const guard = AppendCondition.create(Query.items(QueryItem.withTags("course:c1")));
        await expect(
          client.append([Event.create("CourseOpened", ["course:c1"])], guard),
        ).rejects.toBeInstanceOf(ServerError);
        try {
          await client.append([Event.create("CourseOpened", ["course:c1"])], guard);
        } catch (err) {
          expect((err as ServerError).code).toBe(ErrorCode.Conflict);
        }
      } finally {
        await client.close();
      }
    }, 60_000);
  },
);
