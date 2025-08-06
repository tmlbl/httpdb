const std = @import("std");
const zin = @import("zinatra");

const storage = @import("./storage.zig");
const Schema = @import("./schema.zig");
const Query = @import("./Query.zig");
const Store = storage.Store;
const json = @import("./json.zig");

const max_row_size = 4096;

var store: ?Store = null;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }
    var app = try zin.App.init(.{
        .allocator = gpa.allocator(),
    });
    defer app.deinit();

    try app.get("/tables", listTablesAll);
    try app.post("/:name", postData);
    try app.get("/:name", readData);
    try app.delete("/:name", deleteData);

    store = try Store.init(.{
        .dirname = "/tmp/httpdb",
        .allocator = gpa.allocator(),
    });
    defer store.?.deinit();

    try app.listen();
}

fn listTablesAll(ctx: *zin.Context) !void {
    return listTables(ctx, null);
}

fn listTables(ctx: *zin.Context, tag: ?[]const u8) !void {
    try ctx.headers.append(.{ .name = "Content-Type", .value = "application/json" });

    if (tag != null) {
        std.debug.print("listing tables for tag {s}\n", .{tag.?});
    }

    const buf = try ctx.allocator().alloc(u8, max_row_size);
    defer ctx.allocator().free(buf);

    var response = ctx.req.respondStreaming(.{
        .send_buffer = buf,
        .respond_options = .{
            .extra_headers = ctx.headers.items,
            .transfer_encoding = .chunked,
        },
    });

    var w = response.writer();
    try w.writeByte('[');

    var it = try storage.SchemaIter.init(
        store.?.allocator,
        store.?.db,
        tag,
    );
    var first = true;
    while (it.next()) |data| {
        if (!first) {
            try w.writeByte(',');
        }
        try w.writeAll(data);
        first = false;
    }

    try w.writeByte(']');
    try response.end();
}

fn postData(ctx: *zin.Context) !void {
    if (ctx.req.head.content_type) |ctype| {
        if (std.mem.eql(u8, ctype, "application/json")) {
            try json.postDataJSON(ctx, &store.?);
        } else if (std.mem.eql(u8, ctype, "text/csv")) {
            try postDataCSV(ctx);
        } else {
            try ctx.fmt(.bad_request, "unsupported content-type: {s}", .{ctype});
        }
    } else {
        try ctx.text(.bad_request, "missing content-type header");
    }
}

fn readData(ctx: *zin.Context) !void {
    const name = ctx.params.get("name").?;
    const p = try store.?.getTable(name);

    if (p == null) {
        try ctx.text(.not_found, "table not found");
        return;
    } else {
        const schema = p.?.value;
        if (schema.dataType == .csv) {
            try readDataCSV(ctx);
        } else if (schema.dataType == .json) {
            try json.readDataJSON(ctx, &store.?);
        }
    }
}

fn postDataCSV(ctx: *zin.Context) !void {
    const name = ctx.params.get("name").?;
    var r = try ctx.req.reader();
    const header = try r.readUntilDelimiterAlloc(
        ctx.allocator(),
        '\n',
        max_row_size,
    );

    // create table if not exists
    if (try store.?.getTable(name) == null) {
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
        try store.?.createTable(schema);
        std.log.debug("created table {s}", .{schema.name});
    }

    // fetch table def and verify header
    const p = try store.?.getTable(name);
    const def = p.?.value;

    var i: usize = 0;
    var it = std.mem.splitAny(u8, header, ",");
    while (it.next()) |col| {
        if (!std.mem.eql(u8, col, def.columns[i])) {
            try ctx.text(.bad_request, try std.fmt.allocPrint(
                ctx.allocator(),
                "unexpected column: {s}",
                .{col},
            ));
            return;
        }
        i += 1;
    }

    // write rows
    const buf = try ctx.allocator().alloc(u8, max_row_size);
    while (true) {
        const row = r.readUntilDelimiter(buf, '\n') catch |err| {
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
        try store.?.writeRow(def.name, pkey, row);
    }

    try ctx.text(.ok, "ok");
}

fn readDataCSV(ctx: *zin.Context) !void {
    const name = ctx.params.get("name").?;
    const query = try Query.fromContext(ctx);

    // fetch table header
    const tdef = try store.?.getTable(name);
    if (tdef == null) {
        try ctx.text(.not_found, "table not found");
        return;
    }

    var it = try store.?.query(name, query);
    defer it.deinit();

    try ctx.headers.append(.{ .name = "Content-Type", .value = "text/csv" });

    const buf = try ctx.allocator().alloc(u8, max_row_size);
    defer ctx.allocator().free(buf);

    var response = ctx.req.respondStreaming(.{
        .send_buffer = buf,
        .respond_options = .{
            .extra_headers = ctx.headers.items,
            .transfer_encoding = .chunked,
        },
    });

    var w = response.writer();

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

    var bw = std.io.BufferedWriter(max_row_size, @TypeOf(w)){
        .unbuffered_writer = w,
    };

    // write rows
    while (it.next()) |row| {
        _ = try bw.write(row);
        _ = try bw.write(&[1]u8{'\n'});
    }

    try bw.flush();

    try response.end();
}

fn deleteData(ctx: *zin.Context) !void {
    const name = ctx.params.get("name").?;
    try store.?.deleteTable(name);
    try ctx.text(.ok, "table deleted");
}
