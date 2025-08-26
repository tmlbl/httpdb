const std = @import("std");
const zin = @import("zinatra");
const clap = @import("clap");

const storage = @import("storage.zig");
const Schema = @import("schema.zig");
const Query = @import("Query.zig");
const Store = storage.Store;
const json = @import("json.zig");
const csv = @import("csv.zig");

const max_row_size = 4096;

var store: ?Store = null;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }

    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\-d, --directory <str>  Directory for the embedded data store
        \\-w, --workers <usize>  Number of worker threads
        \\
    );

    var opts = try clap.parse(clap.Help, &params, clap.parsers.default, .{
        .allocator = gpa.allocator(),
    });
    defer opts.deinit();

    if (opts.args.help != 0) {
        var buf: [512]u8 = undefined;
        var writer = std.fs.File.stderr().writer(&buf).interface;
        try clap.help(&writer, clap.Help, &params, .{});
        std.process.exit(0);
    }

    var nWorkers: usize = 16;
    if (opts.args.workers != null) {
        nWorkers = opts.args.workers.?;
    }

    var dataDir: []const u8 = "/tmp/httpdb";
    if (opts.args.directory != null) {
        dataDir = opts.args.directory.?;
    }

    var app = try zin.App.init(.{
        .allocator = gpa.allocator(),
        .n_workers = nWorkers,
    });
    defer app.deinit();

    try app.get("/tables", listTables);
    try app.post("/:name", postData);
    try app.get("/:name", readData);
    try app.delete("/:name", deleteData);

    store = try Store.init(.{
        .dirname = dataDir,
        .allocator = gpa.allocator(),
    });
    defer store.?.deinit();

    try app.listen();
}

fn listTables(ctx: *zin.Context) !void {
    try ctx.addHeader(.{ .name = "Content-Type", .value = "application/json" });

    const buf = try ctx.allocator().alloc(u8, max_row_size);
    defer ctx.allocator().free(buf);

    var response = try ctx.req.respondStreaming(buf, .{
        .respond_options = .{
            .extra_headers = ctx.headers.items,
            .transfer_encoding = .chunked,
        },
    });

    var w = &response.writer;
    try w.writeByte('[');

    var it = try storage.SchemaIter.init(
        store.?.allocator,
        store.?.db,
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
    try w.flush();
    try response.endChunked(.{});
}

fn postData(ctx: *zin.Context) !void {
    const buf = try ctx.allocator().alloc(u8, 4096);
    var reader = try ctx.req.readerExpectContinue(buf);
    if (ctx.req.head.content_type) |ctype| {
        if (std.mem.eql(u8, ctype, "application/json")) {
            try json.postDataJSON(ctx, &store.?, reader);
        } else if (std.mem.eql(u8, ctype, "text/csv")) {
            try csv.postDataCSV(ctx, &store.?, reader);
        } else {
            // infer datatype from payload
            const byte = try reader.peekByte();
            switch (byte) {
                '[', '{' => try json.postDataJSON(ctx, &store.?, reader),
                else => try csv.postDataCSV(ctx, &store.?, reader),
            }
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
        return switch (schema.dataType) {
            .csv => csv.readDataCSV(ctx, &store.?),
            .json => json.readDataJSON(ctx, &store.?),
        };
    }
}

fn deleteData(ctx: *zin.Context) !void {
    const name = ctx.params.get("name").?;
    var schema = try store.?.getTable(name);
    if (schema == null) {
        try ctx.text(.not_found, "table not found");
        return;
    }
    defer schema.?.deinit();

    const query = try Query.fromContext(ctx);
    try store.?.deleteData(schema.?.value, query);
    try ctx.fmt(.ok, "deleted from table {s}", .{name});
}
