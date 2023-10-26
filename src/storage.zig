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

pub const ScanOptions = struct {
    start: ?[]const u8,
    end: ?[]const u8,
};

pub const RowIter = struct {
    allocator: std.mem.Allocator,
    it: *rdb.rocksdb_iterator_t,
    first: bool = true,
    prefix: []const u8,

    pub fn init(allocator: std.mem.Allocator, db: *rdb.rocksdb_t, prefix: []const u8) !RowIter {
        var readOpts = rdb.rocksdb_readoptions_create();
        var iter = rdb.rocksdb_create_iterator(db, readOpts);
        if (iter == null) {
            return error.CouldNotCreateIterator;
        }
        rdb.rocksdb_iter_seek(iter.?, prefix.ptr, prefix.len);
        var ownedPrefix = try allocator.alloc(u8, prefix.len);
        std.mem.copy(u8, ownedPrefix, prefix);
        return RowIter{
            .allocator = allocator,
            .it = iter.?,
            .prefix = ownedPrefix,
        };
    }

    pub fn deinit(self: *RowIter) void {
        self.allocator.free(self.prefix);
    }

    pub fn next(self: *RowIter) ?[]const u8 {
        if (!self.first) {
            rdb.rocksdb_iter_next(self.it);
        }
        self.first = false;
        if (rdb.rocksdb_iter_valid(self.it) != 1) {
            return null;
        }

        var keySize: usize = 0;
        var rawKey = rdb.rocksdb_iter_key(self.it, &keySize);
        if (!std.mem.startsWith(u8, rawKey[0..keySize], self.prefix)) {
            return null;
        }

        var valueSize: usize = 0;
        var rawValue = rdb.rocksdb_iter_value(self.it, &valueSize);
        return rawValue[0..valueSize];
    }
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
            "table_def:{s}",
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

    fn rowKey(self: *Store, table: []const u8, pkey: []const u8) ![]const u8 {
        return std.fmt.allocPrint(
            self.allocator,
            "row:{s}:{s}",
            .{ table, pkey },
        );
    }

    pub fn writeRow(self: *Store, table: []const u8, data: []const u8) !void {
        var cix = std.mem.indexOf(u8, data, ",");
        if (cix == null) {
            // tables with only one row will have no comma
            cix = data.len;
        }
        var pkey = data[0..cix.?];
        var key = try self.rowKey(table, pkey);
        defer self.allocator.free(key);

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
            std.log.err("writing row: {s}", .{str});
            return error.Cerror;
        }
    }

    pub fn scanRows(self: *Store, table: []const u8) !RowIter {
        var prefix = try self.rowKey(table, "");
        defer self.allocator.free(prefix);
        return RowIter.init(
            self.allocator,
            self.db,
            prefix,
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

test "table defs" {
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

test "scan rows" {
    var td = try TestDB.init();
    defer td.deinit();

    try td.s.writeRow("foo", "bar");
    try td.s.writeRow("foo", "baz");

    var it = try td.s.scanRows("foo");
    while (it.next()) |row| {
        std.debug.print("scanned row: {s}\n", .{row});
    }
}
