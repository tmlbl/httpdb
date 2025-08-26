const std = @import("std");

const chars = "1234567890qwertyuiopasdfghjklzxcvbnm";
var rng: ?std.Random.Xoshiro256 = null;

fn getRNG() std.Random {
    if (rng == null) {
        rng = std.Random.DefaultPrng.init(
            @as(u64, @intCast(std.time.microTimestamp())),
        );
    }
    return rng.?.random();
}

pub fn randomChars(buf: []u8) void {
    for (buf, 0..) |_, i| {
        const cix = std.Random.uintLessThan(getRNG(), usize, chars.len);
        buf[i] = chars[cix];
    }
}

pub fn randomCharsAlloc(a: std.mem.Allocator, n: usize) ![]const u8 {
    var str = try a.alloc(u8, n);
    for (str, 0..) |_, i| {
        const cix = std.Random.uintLessThan(getRNG(), usize, chars.len);
        str[i] = chars[cix];
    }
    return str;
}

pub fn tempDir(a: std.mem.Allocator, prefix: []const u8) ![:0]u8 {
    const buf = try a.alloc(u8, 10);
    randomChars(buf);
    const p = try std.fmt.allocPrintSentinel(a, "/tmp/{s}-{s}", .{ prefix, buf }, 0);
    a.free(buf);
    return p;
}
