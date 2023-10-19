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

pub const TableDefinition = struct {
    name: []const u8,
    columns: []const []const u8,
};

pub const DB = struct {
    db: *rdb.rocksdb_t,
    allocator: std.mem.Allocator,

    pub fn init(opts: Options) !DB {
        var options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);

        std.debug.print("\nopening {s}...\n", .{opts.dirname});
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
        return DB{ .db = db.?, .allocator = opts.allocator };
    }

    fn tableKey(self: *DB, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(
            self.allocator,
            "table:{s}",
            .{name},
        );
    }

    pub fn createTable(self: *DB, def: *TableDefinition) !void {
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

    pub fn getTable(self: *DB, name: []const u8) !?std.json.Parsed(TableDefinition) {
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
        return try std.json.parseFromSlice(TableDefinition, self.allocator, v[0..valueLength], .{});
    }
};

test "in this file" {
    try std.testing.expect(1 == 2);
}
