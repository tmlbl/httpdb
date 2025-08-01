const std = @import("std");
const Schema = @import("./schema.zig");
const Query = @import("./Query.zig");
const rdb = @cImport(@cInclude("rocksdb/c.h"));

pub const RowIter = @This();

allocator: std.mem.Allocator,
it: *rdb.rocksdb_iterator_t,
first: bool = true,
prefix: []const u8,
dt: Schema.DataType,
query: ?Query = null,

pub fn init(
    allocator: std.mem.Allocator,
    db: ?*rdb.rocksdb_t,
    prefix: []const u8,
    dt: Schema.DataType,
    q: ?Query,
) !RowIter {
    const readOpts = rdb.rocksdb_readoptions_create();
    const iter = rdb.rocksdb_create_iterator(db, readOpts);
    if (iter == null) {
        return error.CouldNotCreateIterator;
    }
    rdb.rocksdb_iter_seek(iter.?, prefix.ptr, prefix.len);
    const ownedPrefix = try allocator.alloc(u8, prefix.len);
    std.mem.copyForwards(u8, ownedPrefix, prefix);
    return RowIter{
        .allocator = allocator,
        .it = iter.?,
        .prefix = ownedPrefix,
        .dt = dt,
        .query = q,
    };
}

pub fn deinit(self: *RowIter) void {
    self.allocator.free(self.prefix);
    rdb.rocksdb_iter_destroy(self.it);
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
    const value = rawValue[0..valueSize];

    if (self.query == null) {
        return value;
    } else if (self.dt == .json and !self.query.?.testValueJson(value)) {
        return self.next();
    }

    return value;
}
