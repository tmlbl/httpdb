const std = @import("std");
const Schema = @import("./schema.zig");
const Query = @import("./Query.zig");
const rdb = @import("rocksdb.zig").rdb;

pub const RowIter = @This();

allocator: std.mem.Allocator,
it: *rdb.rocksdb_iterator_t,
first: bool = true,
prefix: []const u8,
schema: Schema,
query: ?*Query = null,

pub fn init(
    allocator: std.mem.Allocator,
    db: ?*rdb.rocksdb_t,
    schema: Schema,
    q: ?*Query,
) !RowIter {
    const readOpts = rdb.rocksdb_readoptions_create();
    const iter = rdb.rocksdb_create_iterator(db, readOpts);
    if (iter == null) {
        return error.CouldNotCreateIterator;
    }
    const prefix = try tablePrefix(
        allocator,
        schema,
        getSeekKey(q, schema),
    );
    rdb.rocksdb_iter_seek(iter.?, prefix.ptr, prefix.len);
    return RowIter{
        .allocator = allocator,
        .it = iter.?,
        .prefix = prefix,
        .schema = schema,
        .query = q,
    };
}

pub fn deinit(self: *RowIter) void {
    self.allocator.free(self.prefix);
    rdb.rocksdb_iter_destroy(self.it);
}

fn tablePrefix(allocator: std.mem.Allocator, schema: Schema, pkey: []const u8) ![]const u8 {
    return std.fmt.allocPrint(
        allocator,
        "row:{s}:{s}",
        .{ schema.name, pkey },
    );
}

fn getSeekKey(query: ?*Query, schema: Schema) []const u8 {
    if (query != null) {
        for (query.?.clauses.items) |clause| {
            if (schema.dataType == .csv and
                std.mem.eql(u8, clause.lhs, schema.columns[0]))
            {
                if (clause.comparator == .equal) {
                    return clause.rhs;
                }
            }
            if (schema.dataType == .json and
                std.mem.eql(u8, clause.lhs, "id"))
            {
                if (clause.comparator == .equal) {
                    return clause.rhs;
                }
            }
        }
    }
    return "";
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
    const keySlice = rawKey[0..keySize];
    if (!std.mem.startsWith(u8, keySlice, self.prefix)) {
        return null;
    }

    var valueSize: usize = 0;
    var rawValue = rdb.rocksdb_iter_value(self.it, &valueSize);
    const value = rawValue[0..valueSize];

    if (self.query == null) {
        return value;
    }

    const match = switch (self.schema.dataType) {
        .json => self.query.?.testValueJson(value),
        .csv => self.query.?.testValueCsv(self.schema, value),
    } catch |err| {
        std.log.err("error evaluating query: {}", .{err});
        return value;
    };

    if (!match) {
        return self.next();
    }

    return value;
}
