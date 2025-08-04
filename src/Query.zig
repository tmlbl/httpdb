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
        self.a.free(clause.lhs);
        self.a.free(clause.rhs);
    }
    self.clauses.deinit();
}

pub const Comparator = enum {
    equal,
    greater_than,
    less_than,
};

pub const Clause = struct {
    lhs: []const u8,
    rhs: []const u8,
    comparator: Comparator,
};

pub fn matchesSlice(self: Clause, s: []const u8) bool {
    return switch (self.comparator) {
        .equal => std.mem.eql(u8, s, self.rhs),
        .less_than => std.mem.lessThan(u8, s, self.rhs),
        .greater_than => !std.mem.lessThan(u8, s, self.rhs) and
            !std.mem.eql(u8, s, self.rhs),
    };
}

pub fn matchesNumber(self: Clause, s: []const u8) !bool {
    const num = try std.fmt.parseFloat(f64, s);
    const rhsNum = std.fmt.parseFloat(f64, self.rhs) catch |err| {
        std.log.err("bad numeric rhs: {}", .{err});
        return error.ComparingNonNumberToNumber;
    };
    return switch (self.comparator) {
        .equal => num == rhsNum,
        .less_than => num < rhsNum,
        .greater_than => num > rhsNum,
    };
}

pub fn fromQueryString(a: std.mem.Allocator, s: []const u8) !?Query {
    var query = Query.init(a);

    var clauseIt = std.mem.splitAny(u8, s, "&");
    while (clauseIt.next()) |clauseStr| {
        var it = std.mem.splitAny(u8, clauseStr, "<>=");
        const lhs = it.first();
        const rhs = it.next();
        if (rhs == null) {
            return error.NoValueForKey;
        }
        const comparatorByte = clauseStr[lhs.len];

        try query.clauses.append(.{
            .lhs = try a.dupe(u8, lhs),
            .rhs = try a.dupe(u8, rhs.?),
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

pub fn testValueJson(self: Query, value: []const u8) !bool {
    var scanner = std.json.Scanner.initCompleteInput(self.a, value);
    defer scanner.deinit();

    var clausesMatch = try self.a.alloc(bool, self.clauses.items.len);
    defer self.a.free(clausesMatch);

    while (true) {
        const token = try scanner.next();
        switch (token) {
            .end_of_document => break,
            .string => {
                var clauseIndex: usize = 0;
                for (self.clauses.items) |clause| {
                    if (std.mem.eql(u8, token.string, clause.lhs)) {
                        const valueToken = try scanner.next();
                        clausesMatch[clauseIndex] = switch (valueToken) {
                            .string => matchesSlice(clause, valueToken.string),
                            .number => try matchesNumber(clause, valueToken.number),
                            else => false,
                        };
                    }

                    clauseIndex += 1;
                }
            },
            else => {
                var allMatch = true;
                for (clausesMatch) |match| {
                    if (!match) allMatch = false;
                }
                if (allMatch) return allMatch;
            },
        }
    }

    return false;
}

test "single clause" {
    const s = "foo=bar";
    const q: ?Query = try fromQueryString(std.testing.allocator, s);
    defer q.?.deinit();

    try std.testing.expectEqual(1, q.?.clauses.items.len);

    const clause = q.?.clauses.getLast();

    try std.testing.expectEqualSlices(u8, "foo", clause.lhs);
    try std.testing.expectEqualSlices(u8, "bar", clause.rhs);
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

test "simple equals query json" {
    var query = Query.init(std.testing.allocator);
    defer query.deinit();

    try query.clauses.append(Clause{
        .comparator = .equal,
        .lhs = try query.a.dupe(u8, "kind"),
        .rhs = try query.a.dupe(u8, "a"),
    });

    const match =
        \\{"id":"1","kind":"a"}
    ;
    try std.testing.expect(try query.testValueJson(match));

    const noMatch =
        \\{"id":"2","kind":"b"}
    ;
    try std.testing.expect(!try query.testValueJson(noMatch));
}

test "compound query json" {
    var query = Query.init(std.testing.allocator);
    defer query.deinit();

    try query.clauses.append(Clause{
        .comparator = .equal,
        .lhs = try query.a.dupe(u8, "kind"),
        .rhs = try query.a.dupe(u8, "a"),
    });

    try query.clauses.append(Clause{
        .comparator = .greater_than,
        .lhs = try query.a.dupe(u8, "id"),
        .rhs = try query.a.dupe(u8, "1"),
    });

    const idTooSmall =
        \\{"id":"1","kind":"a"}
    ;
    try std.testing.expect(!try query.testValueJson(idTooSmall));

    const match =
        \\{"id":"2","kind":"a"}
    ;
    try std.testing.expect(try query.testValueJson(match));
}

test "bad query string" {
    try std.testing.expectError(
        error.NoValueForKey,
        Query.fromQueryString(std.testing.allocator, "foo?bar"),
    );
}

test "numeric query json" {
    var query = Query.init(std.testing.allocator);
    defer query.deinit();

    try query.clauses.append(Clause{
        .comparator = .less_than,
        .lhs = try query.a.dupe(u8, "num"),
        .rhs = try query.a.dupe(u8, "17"),
    });

    const match =
        \\{"num":100}
    ;
    try std.testing.expect(!try query.testValueJson(match));

    const noMatch =
        \\{"num":9}
    ;
    try std.testing.expect(try query.testValueJson(noMatch));
}

test "mixed value types" {
    const s = "foo=bar";
    const q: ?Query = try fromQueryString(std.testing.allocator, s);
    defer q.?.deinit();
    const query = q.?;

    try std.testing.expect(try query.testValueJson(
        \\{"foo":"bar"}
    ));

    // If the type of the value is not supported, we can just not match it
    // instead of returning an error, which would fail the entire request
    try std.testing.expect(!try query.testValueJson(
        \\{"foo":{}}
    ));
}
