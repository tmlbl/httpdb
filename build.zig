const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .root_source_file = .{ .cwd_relative = "src/main.zig" },
    });

    const exe = b.addExecutable(.{
        .name = "httpdb",
        .root_module = mod,
    });

    const clap = b.dependency("clap", .{});
    exe.root_module.addImport("clap", clap.module("clap"));

    exe.linkLibC();
    exe.linkSystemLibrary("rocksdb");

    const zinatra = b.addModule("zinatra", .{
        .root_source_file = .{ .cwd_relative = "./zinatra/src/root.zig" },
    });
    exe.root_module.addImport("zinatra", zinatra);

    b.installArtifact(exe);

    const tests = b.addTest(.{
        .root_module = mod,
    });
    tests.linkLibC();
    tests.linkSystemLibrary("rocksdb");

    const run_tests = b.addRunArtifact(tests);

    const test_step = b.step("test", "Run the tests");
    test_step.dependOn(&run_tests.step);
}
