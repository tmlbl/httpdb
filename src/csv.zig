const max_row_size = 4096;

pub fn postDataCSV(ctx: *zin.Context, store: *storage.Store) !void {
    const name = ctx.params.get("name").?;
    const buf = try ctx.allocator().alloc(u8, max_row_size);
    var r = try ctx.req.readerExpectContinue(buf);
    const header = try r.takeDelimiterExclusive(
        '\n',
    );

    // create table if not exists
    if (try store.getTable(name) == null) {
        const ncols = std.mem.count(u8, header, ",") + 1;
        var schema = try ctx.allocator().create(Schema);
        schema.name = name;
        var columns = try ctx.allocator().alloc([]const u8, ncols);
        var i: usize = 0;
        var it = std.mem.splitAny(u8, header, ",");
        while (it.next()) |col| {
            columns[i] = col;
            i += 1;
        }
        schema.columns = columns;
        try store.createTable(schema);
        std.log.debug("created table {s}", .{schema.name});
    }

    // fetch table def and verify header
    const p = try store.getTable(name);
    const def = p.?.value;

    var i: usize = 0;
    var it = std.mem.splitAny(u8, header, ",");
    while (it.next()) |col| {
        if (!std.mem.eql(u8, col, def.columns[i])) {
            try ctx.fmt(.bad_request, "unexpected column: {s}", .{col});
            return;
        }
        i += 1;
    }

    // write rows
    while (true) {
        const row = r.takeDelimiterExclusive('\n') catch |err| {
            if (err == error.EndOfStream) {
                break;
            } else {
                return err;
            }
        };
        var cix = std.mem.indexOf(u8, row, ",");
        if (cix == null) {
            // tables with only one row will have no comma
            cix = row.len;
        }
        const pkey = row[0..cix.?];
        try store.writeRow(def.name, pkey, row);
    }

    try ctx.text(.ok, "ok");
}

pub fn readDataCSV(ctx: *zin.Context, store: *storage.Store) !void {
    const name = ctx.params.get("name").?;
    const query = try Query.fromContext(ctx);

    // fetch table header
    const tdef = try store.getTable(name);
    if (tdef == null) {
        try ctx.text(.not_found, "table not found");
        return;
    }

    var it = try store.query(name, query);
    defer it.deinit();

    try ctx.addHeader(.{ .name = "Content-Type", .value = "text/csv" });

    const buf = try ctx.allocator().alloc(u8, max_row_size);
    defer ctx.allocator().free(buf);

    var response = try ctx.req.respondStreaming(buf, .{
        .respond_options = .{
            .extra_headers = ctx.headers.items,
            .transfer_encoding = .chunked,
        },
    });

    var w = response.writer;

    // write header
    const cols = tdef.?.value.columns;
    for (0..cols.len) |i| {
        try w.writeAll(cols[i]);
        if (i < (cols.len - 1)) {
            try w.writeByte(',');
        } else {
            try w.writeByte('\n');
        }
    }

    // write rows
    while (it.next()) |row| {
        _ = try w.write(row);
        _ = try w.write(&[1]u8{'\n'});
    }

    try w.flush();

    try response.end();
}

const Query = @import("Query.zig");
const Schema = @import("schema.zig");
const storage = @import("storage.zig");
const zin = @import("zinatra");
const std = @import("std");
