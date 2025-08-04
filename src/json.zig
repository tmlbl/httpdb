const std = @import("std");
const zin = @import("zinatra");
const Store = @import("./storage.zig").Store;
const Schema = @import("./schema.zig");
const Query = @import("./Query.zig");

const MAX_BODY_SIZE = 4096;

pub fn postDataJSON(ctx: *zin.Context, store: *Store) !void {
    const name = ctx.params.get("name").?;
    // Read and parse
    const reader = try ctx.req.reader();
    if (ctx.req.head.content_length == null) {
        try ctx.text(.bad_request, "missing content-length");
        return;
    }
    const len = ctx.req.head.content_length.?;
    const data = try reader.readAllAlloc(ctx.allocator(), len);

    writeData(ctx.allocator(), store, name, data) catch |err| {
        switch (err) {
            error.NoId => try ctx.text(.bad_request, "json objects must have \"id\" key"),
            else => try ctx.fmt(.internal_server_error, "error: {}", .{err}),
        }
    };

    try ctx.text(.ok, "wrote data");
}

fn writeData(
    allocator: std.mem.Allocator,
    store: *Store,
    table_name: []const u8,
    data: []const u8,
) !void {
    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        allocator,
        data,
        .{},
    );
    defer parsed.deinit();

    // Singular value
    const root = parsed.value;

    switch (root) {
        .object => return writeJsonObject(allocator, store, table_name, root),
        .array => {
            for (root.array.items) |value| {
                switch (value) {
                    .object => return writeJsonObject(allocator, store, table_name, value),
                    else => return error.NonObjectItemInArray,
                }
            }
        },
        else => return error.NonObjectOrArray,
    }
}

fn writeJsonObject(
    allocator: std.mem.Allocator,
    store: *Store,
    table_name: []const u8,
    value: std.json.Value,
) !void {
    const object = value.object;

    if (object.get("id") == null) {
        return error.NoId;
    }

    // Check if table exists
    if (try store.getTable(table_name) == null) {
        // Use top-level keys as the column list
        var schema = Schema{
            .name = table_name,
            .dataType = .json,
            .columns = object.keys(),
        };
        store.createTable(&schema) catch |err| switch (err) {
            error.AlreadyExists => {},
            else => return err,
        };
    }

    const pkey = object.get("id").?.string;

    const data = try std.json.stringifyAlloc(allocator, value, .{});
    defer allocator.free(data);

    try store.writeRow(table_name, pkey, data);
}

pub fn readDataJSON(ctx: *zin.Context, store: *Store) !void {
    const name = ctx.params.get("name").?;
    const query = try Query.fromContext(ctx);

    try ctx.headers.append(.{ .name = "Content-Type", .value = "application/json" });

    const buf = try ctx.allocator().alloc(u8, 4096);

    var response = ctx.req.respondStreaming(.{
        .send_buffer = buf,
        .respond_options = .{
            .extra_headers = ctx.headers.items,
            .transfer_encoding = .chunked,
        },
    });

    const w = response.writer();

    const startTime = std.time.milliTimestamp();
    try scanRows(store, name, query, w);
    const elapsed = std.time.milliTimestamp() - startTime;

    std.log.debug("query took {d}ms", .{elapsed});

    try response.endChunked(.{});
}

fn scanRows(
    store: *Store,
    tableName: []const u8,
    query: ?Query,
    writer: std.io.AnyWriter,
) !void {
    var it = try store.query(tableName, query);
    defer it.deinit();

    var bw = std.io.BufferedWriter(4096, @TypeOf(writer)){
        .unbuffered_writer = writer,
    };

    // write JSON array
    var first = true;
    _ = try bw.write(&[1]u8{'['});
    while (it.next()) |row| {
        if (first) {
            first = false;
        } else {
            _ = try bw.write(&[1]u8{','});
        }
        _ = try bw.write(row);
    }
    _ = try bw.write(&[1]u8{']'});

    try bw.flush();
}

const TestDB = @import("./storage.zig").TestDB;

test "write single value" {
    var store = try TestDB.init();
    defer store.deinit();

    const table_name = "people";

    const data =
        \\ { "id": "tim" }
    ;

    try writeData(
        std.testing.allocator,
        &store.s,
        table_name,
        data,
    );
}

test "write bad data" {
    var store = try TestDB.init();
    defer store.deinit();

    const table_name = "bad";

    const data = "3";

    try std.testing.expectError(error.NonObjectOrArray, writeData(
        std.testing.allocator,
        &store.s,
        table_name,
        data,
    ));
}

test "write an array" {
    var store = try TestDB.init();
    defer store.deinit();

    const tableName = "arraytest";

    const data =
        \\[{"id": "a"},{"id":"b"}]
    ;

    try writeData(
        std.testing.allocator,
        &store.s,
        tableName,
        data,
    );
}
