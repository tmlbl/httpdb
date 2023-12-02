const std = @import("std");

name: []const u8,
dataType: DataType,
columns: []const []const u8,

pub const DataType = enum {
    csv,
    json,
};
