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
        try clap.help(std.io.getStdErr().writer(), clap.Help, &params, .{});
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

    try app.get("/tables", listTablesAll);
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
            try csv.postDataCSV(ctx, &store.?);
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
