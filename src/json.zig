const std = @import("std");
const zin = @import("zinatra");
const Store = @import("./storage.zig").Store;
const Schema = @import("./schema.zig");

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

    // Singular value
    const root = parsed.value;

    if (root.object.get("id") == null) {
        return error.NoId;
    }

    // Check if table exists
    if (try store.getTable(table_name) == null) {
        // Use top-level keys as the column list
        var schema = Schema{
            .name = table_name,
            .dataType = .json,
            .columns = root.object.keys(),
        };
        try store.createTable(&schema);
        std.log.debug("created table {s}", .{schema.name});
    }

    const pkey = root.object.get("id").?.string;

    try store.writeRow(table_name, pkey, data);
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
        std.heap.page_allocator,
        &store.s,
        table_name,
        data,
    );
}
