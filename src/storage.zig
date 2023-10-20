const std = @import("std");
const rdb = @cImport(@cInclude("rocksdb/c.h"));

// supported data types for values
pub const DataType = enum {
    float32,
    float64,
};

pub const Options = struct {
    allocator: std.mem.Allocator,
    dirname: []const u8,
};

pub const TableDef = struct {
    name: []const u8,
    columns: []const []const u8,
};

// Store defines the on-disk storage system for csvd
pub const Store = struct {
    db: *rdb.rocksdb_t,
    allocator: std.mem.Allocator,

    pub fn init(opts: Options) !Store {
        var options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);

        var err: ?[*:0]u8 = null;
        var db: ?*rdb.rocksdb_t = rdb.rocksdb_open(
            options,
            opts.dirname.ptr,
            &err,
        );
        if (err) |ptr| {
            var str = std.mem.span(ptr);
            std.debug.print("ERRR {s}\n", .{str});
            return error.AnyError;
        }

        std.log.info("opened rocksdb database: {s}", .{opts.dirname});
        return Store{ .db = db.?, .allocator = opts.allocator };
    }

    fn tableKey(self: *Store, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(
            self.allocator,
            "table:{s}",
            .{name},
        );
    }

    pub fn createTable(self: *Store, def: *TableDef) !void {
        var key = try self.tableKey(def.name);
        defer self.allocator.free(key);

        // check if exists
        var td = try self.getTable(def.name);
        if (td != null) {
            td.?.deinit();
            return error.AlreadyExists;
        }

        var data = try std.json.stringifyAlloc(
            self.allocator,
            def,
            .{},
        );
        defer self.allocator.free(data);

        var writeOpts = rdb.rocksdb_writeoptions_create();
        var err: ?[*:0]u8 = null;
        rdb.rocksdb_put(
            self.db,
            writeOpts,
            key.ptr,
            key.len,
            data.ptr,
            data.len,
            &err,
        );
        if (err) |ptr| {
            var str = std.mem.span(ptr);
            std.log.err("writing table definition: {s}", .{str});
            return error.Cerror;
        }
    }

    pub fn getTable(self: *Store, name: []const u8) !?std.json.Parsed(TableDef) {
        var key = try self.tableKey(name);
        defer self.allocator.free(key);

        var readOptions = rdb.rocksdb_readoptions_create();
        var valueLength: usize = 0;
        var err: ?[*:0]u8 = null;

        var v = rdb.rocksdb_get(
            self.db,
            readOptions,
            key.ptr,
            key.len,
            &valueLength,
            &err,
        );
        if (err) |ptr| {
            var str = std.mem.span(ptr);
            std.log.err("reading table definition: {s}", .{str});
            return error.Cerror;
        }
        if (v == null) {
            return null;
        }
        return try std.json.parseFromSlice(
            TableDef,
            self.allocator,
            v[0..valueLength],
            .{},
        );
    }
};

const utils = @import("./utils.zig");
const t = std.testing;

const TestDB = struct {
    s: Store,
    dirname: [:0]u8,

    pub fn init() !TestDB {
        var dn = try utils.tempDir(t.allocator, "csvd-test-store");
        return TestDB{
            .s = try Store.init(.{
                .allocator = t.allocator,
                .dirname = dn,
            }),
            .dirname = dn,
        };
    }

    pub fn deinit(self: *TestDB) void {
        std.fs.deleteTreeAbsolute(self.dirname) catch unreachable;
        std.debug.print("deleted directory {s}\n", .{self.dirname});
        t.allocator.free(self.dirname);
    }
};

test "data types" {
    var td = try TestDB.init();
    defer td.deinit();

    var table = TableDef{
        .name = "my_table",
        .columns = &[_][]const u8{ "foo", "bar" },
    };

    // create a table
    try td.s.createTable(&table);

    // fetch it back from disk
    var fetched = try td.s.getTable(table.name);
    defer fetched.?.deinit();
    var ftable = fetched.?.value;

    try t.expect(std.mem.eql(u8, ftable.name, table.name));
    try t.expect(ftable.columns.len == table.columns.len);
    for (0..table.columns.len) |i| {
        try t.expect(std.mem.eql(u8, table.columns[i], ftable.columns[i]));
    }
}
