const std = @import("std");
const zin = @import("zinatra");

const storage = @import("./storage.zig");
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

    try app.post("/tables/:name", postData);
    try app.get("/tables/:name", readData);

    store = try Store.init(.{
        .dirname = "/tmp/csvd",
        .allocator = gpa.allocator(),
    });
    defer store.?.deinit();

    try app.listen();
}

fn version(ctx: *zin.Context) !void {
    try ctx.text("v0.0.1");
}

fn postData(ctx: *zin.Context) !void {
    var name = ctx.params.get("name").?;
    var r = ctx.res.reader();
    var header = try r.readUntilDelimiterAlloc(
        ctx.allocator(),
        '\n',
        max_row_size,
    );

    // create table if not exists
    if (try store.?.getTable(name) == null) {
        var ncols = std.mem.count(u8, header, ",") + 1;
        var ndef = try ctx.allocator().create(storage.TableDef);
        ndef.name = name;
        var columns = try ctx.allocator().alloc([]const u8, ncols);
        var i: usize = 0;
        var it = std.mem.split(u8, header, ",");
        while (it.next()) |col| {
            columns[i] = col;
            i += 1;
        }
        ndef.columns = columns;
        try store.?.createTable(ndef);
        std.log.debug("created table {s}", .{ndef.name});
    }

    // fetch table def and verify header
    var p = try store.?.getTable(name);
    var def = p.?.value;

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
    var buf = try ctx.allocator().alloc(u8, max_row_size);
    while (true) {
        var row = r.readUntilDelimiter(buf, '\n') catch |err| {
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
    var name = ctx.params.get("name").?;

    // fetch table header
    var tdef = try store.?.getTable(name);
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
    var cols = tdef.?.value.columns;
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
