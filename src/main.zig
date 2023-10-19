const std = @import("std");
const zin = @import("zinatra");

const utils = @import("./utils.zig");
const rdb = @import("./DB.zig");
const DB = rdb.DB;

const max_row_size = 4096;

var db: ?DB = null;

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

    db = try DB.init(.{ .dirname = "/tmp/testdb", .allocator = gpa.allocator() });

    try app.listen();
}

fn version(ctx: *zin.Context) !void {
    try ctx.text("v0.0.1");
}

fn postData(ctx: *zin.Context) !void {
    var name = ctx.params.get("name").?;
    var r = ctx.res.reader();
    var header = try r.readUntilDelimiterAlloc(ctx.arena.allocator(), '\n', max_row_size);
    std.log.debug("posting data to table {s} with header: {s}", .{ name, header });
}

test "data types" {
    var temp = try utils.tempDir(std.testing.allocator, "rockstest");
    defer std.testing.allocator.free(temp);
    var td = try DB.init(.{
        .dirname = temp,
        .allocator = std.testing.allocator,
    });
    var table = rdb.TableDefinition{
        .name = "my_table",
        .columns = &[_][]const u8{ "foo", "bar" },
    };
    try td.createTable(&table);
}
