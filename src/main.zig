const std = @import("std");
const zin = @import("zinatra");

const storage = @import("./storage.zig");
const Schema = @import("./schema.zig");
const Store = storage.Store;

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

    try app.get("/version", version);

    try app.get("/tables", listTables);
    try app.post("/tables/:name", postData);
    try app.get("/tables/:name", readData);
    try app.delete("/tables/:name", deleteData);

    store = try Store.init(.{
        .dirname = "/tmp/httpdb",
        .allocator = gpa.allocator(),
    });
    defer store.?.deinit();

    try app.listen();
}

fn version(ctx: *zin.Context) !void {
    try ctx.text("v0.0.1");
}

fn listTablesJSON(ctx: *zin.Context) !void {
    ctx.res.transfer_encoding = .chunked;
    try ctx.res.headers.append("Content-Type", "application/json");
    try ctx.res.send();

    var w = ctx.res.writer();
    try w.writeByte('[');

    var it = try store.?.scanDefinitions();
    var first = true;
    while (it.next()) |data| {
        if (!first) {
            try w.writeByte(',');
        }
        try w.writeAll(data);
        first = false;
    }

    try w.writeByte(']');
    try ctx.res.finish();
}

fn listTables(ctx: *zin.Context) !void {
    const accept = ctx.req.headers.getFirstEntry("Accept");
    if (accept != null) {
        if (std.mem.eql(u8, accept.?.value, "application/json")) {
            return listTablesJSON(ctx);
        } else if (!std.mem.eql(u8, accept.?.value, "text/csv")) {
            if (!std.mem.eql(u8, accept.?.value, "*/*")) {
                try ctx.statusText(
                    std.http.Status.bad_request,
                    "unsupported accept header",
                );
                return;
            }
        }
    }

    ctx.res.transfer_encoding = .chunked;
    try ctx.res.headers.append("Content-Type", "text/csv");
    try ctx.res.send();

    // write header
    var w = ctx.res.writer();
    try w.writeAll("name,columns,dataType\n");

    var it = try store.?.scanDefinitions();
    while (it.next()) |data| {
        const parsed = try std.json.parseFromSlice(
            Schema,
            ctx.allocator(),
            data,
            .{},
        );
        try w.writeAll(parsed.value.name);
        try w.writeByte(',');

        for (0..parsed.value.columns.len) |i| {
            try w.writeAll(parsed.value.columns[i]);
            if (i < parsed.value.columns.len - 1) {
                try w.writeByte('|');
            }
        }
        try w.writeByte(',');

        try w.writeAll(@tagName(parsed.value.dataType));

        try w.writeByte('\n');
    }

    try ctx.res.finish();
}

fn postData(ctx: *zin.Context) !void {
    const name = ctx.params.get("name").?;
    var r = ctx.res.reader();
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
        var it = std.mem.split(u8, header, ",");
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
    var it = std.mem.split(u8, header, ",");
    while (it.next()) |col| {
        if (!std.mem.eql(u8, col, def.columns[i])) {
            try ctx.statusText(.bad_request, try std.fmt.allocPrint(
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
        try store.?.writeRow(def.name, row);
    }

    try ctx.text("ok");
}

fn readData(ctx: *zin.Context) !void {
    const name = ctx.params.get("name").?;

    // fetch table header
    const tdef = try store.?.getTable(name);
    if (tdef == null) {
        try ctx.statusText(std.http.Status.not_found, "table not found");
        return;
    }

    var it = try store.?.scanRows(name);
    defer it.deinit();

    ctx.res.transfer_encoding = .chunked;
    try ctx.res.headers.append("Content-Type", "text/csv");
    try ctx.res.send();

    var w = ctx.res.writer();

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

    var bw = std.io.BufferedWriter(std.mem.page_size, @TypeOf(w)){
        .unbuffered_writer = w,
    };

    // write rows
    while (it.next()) |row| {
        _ = try bw.write(row);
        _ = try bw.write(&[1]u8{'\n'});
    }

    try bw.flush();

    try ctx.res.finish();
}

fn deleteData(ctx: *zin.Context) !void {
    const name = ctx.params.get("name").?;
    try store.?.deleteTable(name);
    try ctx.text("table deleted");
}
