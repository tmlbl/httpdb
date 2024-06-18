const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "httpdb",
        .root_source_file = .{ .cwd_relative = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });

    exe.linkLibC();
    exe.linkSystemLibrary("rocksdb");
    exe.addIncludePath(.{ .cwd_relative = "/usr/include" });

    const zinatra = b.addModule("zinatra", .{
        .root_source_file = .{ .cwd_relative = "./zinatra/src/App.zig" },
    });
    exe.root_module.addImport("zinatra", zinatra);

    b.installArtifact(exe);

    const tests = b.addTest(.{
        .root_source_file = .{ .cwd_relative = "src/main.zig" },
        .target = target,
        .optimize = .Debug,
    });

    const run_tests = b.addRunArtifact(tests);

    const test_step = b.step("test", "Run the tests");
    test_step.dependOn(&run_tests.step);
}
