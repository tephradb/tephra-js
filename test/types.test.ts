import { describe, expect, test } from "vitest";
import { ValidationError } from "../src/errors.js";
import { AppendCondition, Event, MAX_NAME_LEN, Query, QueryItem, ZERO } from "../src/types.js";

describe("Event.create", () => {
  test("stores tags sorted and duplicate-free input intact", () => {
    const event = Event.create("Enrolled", ["student:s1", "course:c1"], new Uint8Array([1]));
    expect(event.type).toBe("Enrolled");
    expect(event.tags).toEqual(["course:c1", "student:s1"]);
    expect(Array.from(event.payload)).toEqual([1]);
  });

  test("defaults tags and payload", () => {
    const event = Event.create("Ping");
    expect(event.tags).toEqual([]);
    expect(event.payload).toHaveLength(0);
  });

  test("rejects an empty type", () => {
    expect(() => Event.create("", [])).toThrow(ValidationError);
  });

  test("rejects an empty tag", () => {
    expect(() => Event.create("T", [""])).toThrow(ValidationError);
  });

  test("rejects a duplicate tag", () => {
    expect(() => Event.create("T", ["a", "a"])).toThrow(ValidationError);
  });

  test("rejects a type over the byte limit", () => {
    expect(() => Event.create("a".repeat(MAX_NAME_LEN + 1))).toThrow(ValidationError);
  });

  test("measures the limit in UTF-8 bytes, not code units", () => {
    // A 2-byte character repeated so the byte length just exceeds the limit.
    const overLong = "é".repeat(Math.floor(MAX_NAME_LEN / 2) + 1);
    expect(() => Event.create(overLong)).toThrow(ValidationError);
  });
});

describe("Query and QueryItem", () => {
  test("Query.all matches everything", () => {
    const query = Query.all();
    expect(query.matchAll).toBe(true);
    expect(query.items).toEqual([]);
  });

  test("Query.items is an OR of items", () => {
    const query = Query.items(QueryItem.withTags("course:c1"), QueryItem.ofTypes("Enrolled"));
    expect(query.matchAll).toBe(false);
    expect(query.items).toHaveLength(2);
  });

  test("QueryItem.ofTypes preserves order and matches any tags", () => {
    const item = QueryItem.ofTypes("B", "A");
    expect(item.types).toEqual(["B", "A"]);
    expect(item.tags).toEqual([]);
  });

  test("QueryItem.withTags sorts tags and rejects duplicates", () => {
    expect(QueryItem.withTags("b:2", "a:1").tags).toEqual(["a:1", "b:2"]);
    expect(() => QueryItem.withTags("a", "a")).toThrow(ValidationError);
  });
});

describe("AppendCondition", () => {
  test("defaults after to ZERO", () => {
    const condition = AppendCondition.create(Query.items(QueryItem.withTags("username:alice")));
    expect(condition.after).toBe(ZERO);
  });

  test("carries an explicit after bound", () => {
    const condition = AppendCondition.create(Query.all(), 42n);
    expect(condition.after).toBe(42n);
  });

  test("has no existence clause by default", () => {
    const condition = AppendCondition.create(Query.all());
    expect(condition.failIfExists).toBeUndefined();
  });

  test("attaches an existence clause to a boundary condition", () => {
    const dedupe = Query.items(QueryItem.withTags("cmd:order-42"));
    const condition = AppendCondition.create(Query.all(), 42n, dedupe);
    expect(condition.after).toBe(42n);
    expect(condition.failIfExists).toBe(dedupe);
  });

  test("existsOnly has a match-nothing boundary and the existence clause", () => {
    const dedupe = Query.items(QueryItem.withTags("cmd:order-42"));
    const condition = AppendCondition.existsOnly(dedupe);
    // A match-nothing boundary (no items), so only the existence clause can fire.
    expect(condition.failIfEventsMatch.matchAll).toBe(false);
    expect(condition.failIfEventsMatch.items).toEqual([]);
    expect(condition.after).toBe(ZERO);
    expect(condition.failIfExists).toBe(dedupe);
  });
});
