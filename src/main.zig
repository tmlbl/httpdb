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
    try app.get("/version", version);

    try app.post("/tables/:name", postData);

    store = try Store.init(.{ .dirname = "/tmp/csvd", .allocator = gpa.allocator() });

    try app.listen();
}

fn version(ctx: *zin.Context) !void {
    try ctx.text("v0.0.1");
}

fn postData(ctx: *zin.Context) !void {
    var name = ctx.params.get("name").?;
    var r = ctx.res.reader();
    var header = try r.readUntilDelimiterAlloc(ctx.allocator(), '\n', max_row_size);

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
            try ctx.err(.bad_request, try std.fmt.allocPrint(
                ctx.allocator(),
                "unexpected column: {s}",
                .{col},
            ));
            return;
        }
        i += 1;
    }

    try ctx.text("ok");
}
