const std = @import("std");
const Schema = @import("./schema.zig");
const Query = @import("./Query.zig");
const RowIter = @import("./RowIter.zig");
const rdb = @cImport(@cInclude("rocksdb/c.h"));

pub const Options = struct {
    allocator: std.mem.Allocator,
    dirname: []const u8,
};

pub const SchemaIter = struct {
    db: ?*rdb.rocksdb_t,
    allocator: std.mem.Allocator,
    it: ?*rdb.rocksdb_iterator_t,
    first: bool = true,
    prefix: []const u8,
    isTagIterator: bool,

    pub fn init(
        allocator: std.mem.Allocator,
        db: ?*rdb.rocksdb_t,
        tag: ?[]const u8,
    ) !SchemaIter {
        const readOpts = rdb.rocksdb_readoptions_create();
        const iter = rdb.rocksdb_create_iterator(db, readOpts);
        if (iter == null) {
            return error.CouldNotCreateIterator;
        }

        const isTag = tag != null;
        var prefix: []u8 = "";
        if (isTag) {
            prefix = try std.fmt.allocPrint(allocator, "tag:{s}:", .{tag.?});
        } else {
            prefix = try std.fmt.allocPrint(allocator, "table_def:", .{});
        }
        rdb.rocksdb_iter_seek(iter.?, prefix.ptr, prefix.len);
        return SchemaIter{
            .db = db,
            .allocator = allocator,
            .it = iter,
            .prefix = prefix,
            .isTagIterator = isTag,
        };
    }

    pub fn deinit(self: *SchemaIter) void {
        self.allocator.free(self.prefix);
        rdb.rocksdb_iter_destroy(self.it);
    }

    pub fn next(self: *SchemaIter) ?[]const u8 {
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

        if (!self.isTagIterator) {
            return rawValue[0..valueSize];
        } else {
            // fetch the actual table schema
            const readOptions = rdb.rocksdb_readoptions_create();
            var valueLength: usize = 0;
            var err: [*c]u8 = null;

            const tableName = rawValue[0..valueSize];
            const key = std.fmt.allocPrint(
                self.allocator,
                "table_def:{s}",
                .{tableName},
            ) catch |e| {
                std.log.err("{any}", .{e});
                return null;
            };
            defer self.allocator.free(key);

            var v = rdb.rocksdb_get(
                self.db,
                readOptions,
                key.ptr,
                key.len,
                &valueLength,
                &err,
            );
            if (err) |ptr| {
                const str = std.mem.span(ptr);
                std.log.err("reading table definition: {s}", .{str});
                return null;
            }
            if (v == null) {
                return null;
            }
            return v[0..valueLength];
        }
    }
};

pub const Store = struct {
    db: ?*rdb.rocksdb_t,
    allocator: std.mem.Allocator,
    mtx: std.Thread.Mutex,
    tableMtx: std.Thread.Mutex,

    pub fn init(opts: Options) !Store {
        const options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);

        var err: [*c]u8 = null;
        const db: ?*rdb.rocksdb_t = rdb.rocksdb_open(
            options,
            opts.dirname.ptr,
            &err,
        );
        if (err) |ptr| {
            const str = std.mem.span(ptr);
            std.debug.print("ERRR {s}\n", .{str});
            return error.AnyError;
        }

        std.log.info("opened rocksdb database: {s}", .{opts.dirname});
        return Store{
            .db = db,
            .allocator = opts.allocator,
            .mtx = std.Thread.Mutex{},
            .tableMtx = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Store) void {
        rdb.rocksdb_close(self.db);
    }

    pub fn put(self: *Store, key: []const u8, value: []const u8) !void {
        self.mtx.lock();
        defer self.mtx.unlock();

        const writeOpts = rdb.rocksdb_writeoptions_create();
        defer rdb.rocksdb_writeoptions_destroy(writeOpts);

        var err: [*c]u8 = null;
        rdb.rocksdb_put(
            self.db,
            writeOpts,
            key.ptr,
            key.len,
            value.ptr,
            value.len,
            &err,
        );
        if (err) |ptr| {
            defer rdb.rocksdb_free(ptr);
            const str = std.mem.span(ptr);
            std.log.err("in rocksdb_put: {s}", .{str});
            return error.Cerror;
        }
    }

    pub fn delete(self: *Store, key: []const u8) !void {
        self.mtx.lock();
        defer self.mtx.unlock();

        const writeOpts = rdb.rocksdb_writeoptions_create();
        var err: [*c]u8 = null;

        rdb.rocksdb_delete(self.db, writeOpts, key.ptr, key.len, &err);
        if (err) |ptr| {
            const str = std.mem.span(ptr);
            std.log.err("deleting row: {s}", .{str});
            return error.Cerror;
        }
    }

    fn tableKey(self: *Store, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(
            self.allocator,
            "table_def:{s}",
            .{name},
        );
    }

    pub fn createTable(self: *Store, schema: *Schema) !void {
        self.tableMtx.lock();
        defer self.tableMtx.unlock();

        const key = try self.tableKey(schema.name);
        defer self.allocator.free(key);

        // check if exists
        var td = try self.getTable(schema.name);
        if (td != null) {
            td.?.deinit();
            return error.AlreadyExists;
        }

        const data = try std.json.stringifyAlloc(
            self.allocator,
            schema,
            .{},
        );
        defer self.allocator.free(data);

        try self.put(key, data);
    }

    pub fn getTable(self: *Store, name: []const u8) !?std.json.Parsed(Schema) {
        self.mtx.lock();
        defer self.mtx.unlock();

        const key = try self.tableKey(name);
        defer self.allocator.free(key);

        const readOptions = rdb.rocksdb_readoptions_create();
        var valueLength: usize = 0;
        var err: [*c]u8 = null;

        var v = rdb.rocksdb_get(
            self.db,
            readOptions,
            key.ptr,
            key.len,
            &valueLength,
            &err,
        );
        if (err) |ptr| {
            const str = std.mem.span(ptr);
            std.log.err("reading table definition: {s}", .{str});
            return error.Cerror;
        }
        if (v == null) {
            return null;
        }
        return try std.json.parseFromSlice(
            Schema,
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

    pub fn writeRow(self: *Store, table: []const u8, pkey: []const u8, data: []const u8) !void {
        // TODO: should not need to allocate here
        const key = try self.rowKey(table, pkey);
        defer self.allocator.free(key);

        try self.put(key, data);
    }

    pub fn query(self: *Store, table: []const u8, q: ?Query) !RowIter {
        const schema = try self.getTable(table);
        defer schema.?.deinit();
        const prefix = try self.rowKey(table, "");
        defer self.allocator.free(prefix);
        return RowIter.init(
            self.allocator,
            self.db,
            prefix,
            schema.?.value,
            q,
        );
    }

    pub fn deleteTable(self: *Store, table: []const u8) !void {
        self.tableMtx.lock();
        defer self.tableMtx.unlock();

        const prefix = try self.rowKey(table, "");
        defer self.allocator.free(prefix);

        const readOpts = rdb.rocksdb_readoptions_create();
        const iter = rdb.rocksdb_create_iterator(self.db, readOpts);
        if (iter == null) {
            return error.CouldNotCreateIterator;
        }
        rdb.rocksdb_iter_seek(iter.?, prefix.ptr, prefix.len);

        var first = true;
        while (rdb.rocksdb_iter_valid(iter) == 1) {
            if (!first) {
                rdb.rocksdb_iter_next(iter);
            }
            first = false;

            var keySize: usize = 0;
            var rawKey = rdb.rocksdb_iter_key(iter, &keySize);
            if (!std.mem.startsWith(u8, rawKey[0..keySize], prefix)) {
                break;
            }

            const writeOpts = rdb.rocksdb_writeoptions_create();
            var err: [*c]u8 = null;
            rdb.rocksdb_delete(self.db, writeOpts, rawKey, keySize, &err);
            if (err) |ptr| {
                const str = std.mem.span(ptr);
                std.log.err("deleting row: {s}", .{str});
                return error.Cerror;
            }
        }
        rdb.rocksdb_iter_destroy(iter);

        const defKey = try self.tableKey(table);
        try self.delete(defKey);
        self.allocator.free(defKey);
    }
};

const utils = @import("./utils.zig");
const t = std.testing;

pub const TestDB = struct {
    s: Store,
    dirname: [:0]u8,

    pub fn init() !TestDB {
        const dn = try utils.tempDir(t.allocator, "test-store");
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

    var table = Schema{
        .name = "my_table",
        .dataType = Schema.DataType.csv,
        .columns = &[_][]const u8{ "foo", "bar" },
    };

    // create a table
    try td.s.createTable(&table);

    // fetch it back from disk
    var fetched = try td.s.getTable(table.name);
    defer fetched.?.deinit();
    const ftable = fetched.?.value;

    try t.expect(std.mem.eql(u8, ftable.name, table.name));
    try t.expect(ftable.columns.len == table.columns.len);
    for (0..table.columns.len) |i| {
        try t.expect(std.mem.eql(u8, table.columns[i], ftable.columns[i]));
    }

    // delete table
    try td.s.deleteTable(table.name);
    try std.testing.expect((try td.s.getTable(table.name)) == null);
}

test "scan rows" {
    var td = try TestDB.init();
    defer td.deinit();

    var schema = Schema{
        .columns = &.{ "a", "b" },
        .dataType = .csv,
        .name = "foo",
    };
    try td.s.createTable(&schema);
    try td.s.writeRow("foo", "bar", "a");
    try td.s.writeRow("foo", "baz", "x");

    var it = try td.s.query("foo", null);
    defer it.deinit();

    var count: u32 = 0;
    while (it.next()) |row| {
        try std.testing.expectEqual(1, row.len);
        count += 1;
    }
    try std.testing.expectEqual(2, count);
}
