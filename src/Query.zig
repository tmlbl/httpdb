const std = @import("std");
const zin = @import("zinatra");

const Query = @This();

a: std.mem.Allocator,
clauses: std.ArrayList(Clause),

fn init(a: std.mem.Allocator) Query {
    return Query{
        .a = a,
        .clauses = std.ArrayList(Clause).init(a),
    };
}

pub fn deinit(self: Query) void {
    for (self.clauses.items) |clause| {
        self.a.free(clause.subject);
        self.a.free(clause.predicate);
    }
    self.clauses.deinit();
}

pub const Comparator = enum {
    equal,
    greater_than,
    less_than,
};

pub const Clause = struct {
    subject: []const u8,
    predicate: []const u8,
    comparator: Comparator,
};

pub fn fromQueryString(a: std.mem.Allocator, s: []const u8) !?Query {
    var query = Query.init(a);

    var clauseIt = std.mem.splitAny(u8, s, "&");
    while (clauseIt.next()) |clauseStr| {
        var it = std.mem.splitAny(u8, clauseStr, "<>=");
        const subject = it.first();
        const predicate = it.next().?;
        const comparatorByte = clauseStr[subject.len];

        try query.clauses.append(.{
            .subject = try a.dupe(u8, subject),
            .predicate = try a.dupe(u8, predicate),
            .comparator = switch (comparatorByte) {
                '<' => .less_than,
                '>' => .greater_than,
                '=' => .equal,
                else => return error.UnsupportedComparator,
            },
        });
    }

    if (query.clauses.items.len > 0) {
        return query;
    } else {
        query.deinit();
        return null;
    }
}

pub fn fromContext(ctx: *zin.Context) !?Query {
    const target = ctx.req.head.target;
    const ix = std.mem.indexOf(u8, target, "?");
    if (ix != null) {
        return fromQueryString(ctx.allocator(), target[(ix.? + 1)..]);
    }
    return null;
}

pub fn testValueJson(_: Query, value: []const u8) bool {
    std.debug.print("testing: {s}\n", .{value});
    return true;
}

test "single clause" {
    const s = "foo=bar";
    const q: ?Query = try fromQueryString(std.testing.allocator, s);
    defer q.?.deinit();

    try std.testing.expectEqual(1, q.?.clauses.items.len);

    const clause = q.?.clauses.getLast();

    try std.testing.expectEqualSlices(u8, "foo", clause.subject);
    try std.testing.expectEqualSlices(u8, "bar", clause.predicate);
    try std.testing.expect(clause.comparator == .equal);
}

test "compound clause" {
    const query = try fromQueryString(
        std.testing.allocator,
        "foo=bar&id>10&x<y",
    );
    defer query.?.deinit();

    const q = query.?;
    try std.testing.expectEqual(3, q.clauses.items.len);

    const gt = q.clauses.items[1];
    try std.testing.expect(gt.comparator == .greater_than);
}
