const std = @import("std");

const chars = "1234567890qwertyuiopasdfghjklzxcvbnm";
var rng: ?std.rand.Xoshiro256 = null;

fn getRNG() std.rand.Random {
    if (rng == null) {
        rng = std.rand.DefaultPrng.init(
            @as(u64, @intCast(std.time.microTimestamp())),
        );
    }
    return rng.?.random();
}

pub fn randomChars(buf: []u8) void {
    for (buf, 0..) |_, i| {
        const cix = std.rand.Random.uintLessThan(getRNG(), usize, chars.len);
        buf[i] = chars[cix];
    }
}

pub fn randomCharsAlloc(a: std.mem.Allocator, n: usize) ![]const u8 {
    var str = try a.alloc(u8, n);
    for (str, 0..) |_, i| {
        const cix = std.rand.Random.uintLessThan(getRNG(), usize, chars.len);
        str[i] = chars[cix];
    }
    return str;
}

pub fn tempDir(a: std.mem.Allocator, prefix: []const u8) ![:0]u8 {
    var buf = try a.alloc(u8, 10);
    randomChars(buf);
    var p = try std.fmt.allocPrintZ(a, "/tmp/{s}-{s}", .{ prefix, buf });
    a.free(buf);
    return p;
}
