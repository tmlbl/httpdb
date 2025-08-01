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
        const predicate = it.next();
        if (predicate == null) {
            return error.NoValueForKey;
        }
        const comparatorByte = clauseStr[subject.len];

        try query.clauses.append(.{
            .subject = try a.dupe(u8, subject),
            .predicate = try a.dupe(u8, predicate.?),
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
                    if (std.mem.eql(u8, token.string, clause.subject)) {
                        const valueToken = try scanner.next();

                        const matches: bool = switch (clause.comparator) {
                            .equal => std.mem.eql(u8, valueToken.string, clause.predicate),
                            .less_than => std.mem.lessThan(u8, valueToken.string, clause.predicate),
                            .greater_than => !std.mem.lessThan(u8, valueToken.string, clause.predicate) and
                                !std.mem.eql(u8, valueToken.string, clause.predicate),
                        };

                        //std.debug.print("clause result: {s} {} {s} => {}\n", .{
                        //    clause.subject, clause.comparator, clause.predicate, matches,
                        //});

                        clausesMatch[clauseIndex] = matches;
                    }

                    clauseIndex += 1;
                }
            },
            else => continue,
        }
    }

    var allMatch = true;
    for (clausesMatch) |match| {
        if (!match) allMatch = false;
    }
    return allMatch;
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

test "simple equals query json" {
    var query = Query.init(std.testing.allocator);
    defer query.deinit();

    try query.clauses.append(Clause{
        .comparator = .equal,
        .subject = try query.a.dupe(u8, "kind"),
        .predicate = try query.a.dupe(u8, "a"),
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
        .subject = try query.a.dupe(u8, "kind"),
        .predicate = try query.a.dupe(u8, "a"),
    });

    try query.clauses.append(Clause{
        .comparator = .greater_than,
        .subject = try query.a.dupe(u8, "id"),
        .predicate = try query.a.dupe(u8, "1"),
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
