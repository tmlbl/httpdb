const std = @import("std");
const zin = @import("zinatra");
const Store = @import("./storage.zig").Store;
const Schema = @import("./schema.zig");
const Query = @import("./Query.zig");

const MAX_BODY_SIZE = 4096;

pub fn postDataJSON(ctx: *zin.Context, store: *Store, reader: *std.Io.Reader) !void {
    const name = ctx.params.get("name").?;
    if (ctx.req.head.content_length == null) {
        try ctx.text(.bad_request, "missing content-length");
        return;
    }
    const len = ctx.req.head.content_length.?;
    const data = try reader.readAlloc(ctx.allocator(), len);

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
                    .object => try writeJsonObject(allocator, store, table_name, value),
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
    const table = try store.getTable(table_name);
    if (table == null) {
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
    } else {
        table.?.deinit();
    }

    const pkey = switch (object.get("id").?) {
        .string => |s| s,
        else => return error.NonStringIdKey,
    };

    const data = try std.json.Stringify.valueAlloc(allocator, value, .{});
    defer allocator.free(data);

    try store.writeRow(table_name, pkey, data);
}

pub fn readDataJSON(ctx: *zin.Context, store: *Store) !void {
    const name = ctx.params.get("name").?;
    const query = try Query.fromContext(ctx);

    try ctx.addHeader(.{ .name = "Content-Type", .value = "application/json" });

    const buf = try ctx.allocator().alloc(u8, 4096);

    var response = try ctx.req.respondStreaming(buf, .{
        .respond_options = .{
            .extra_headers = ctx.headers.items,
            .transfer_encoding = .chunked,
        },
    });

    const startTime = std.time.milliTimestamp();
    try scanRows(store, name, query, &response.writer);
    const elapsed = std.time.milliTimestamp() - startTime;

    std.log.debug("query took {d}ms", .{elapsed});

    try response.endChunked(.{});
}

fn scanRows(
    store: *Store,
    tableName: []const u8,
    query: ?*Query,
    writer: *std.Io.Writer,
) !void {
    var it = try store.query(tableName, query);
    defer it.deinit();

    // write JSON array
    var first = true;
    _ = try writer.write(&[1]u8{'['});
    while (it.next()) |row| {
        if (first) {
            first = false;
        } else {
            _ = try writer.write(&[1]u8{','});
        }
        _ = try writer.write(row);
    }
    _ = try writer.write(&[1]u8{']'});

    try writer.flush();
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

test "non-string key" {
    var store = try TestDB.init();
    defer store.deinit();

    const data =
        \\{"id":1234}
    ;

    try std.testing.expectError(error.NonStringIdKey, writeData(
        std.testing.allocator,
        &store.s,
        "nonstring",
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
