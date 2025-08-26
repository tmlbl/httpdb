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

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    mod_tests.linkLibC();
    mod_tests.linkSystemLibrary("rocksdb");

    const run_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });
    exe_tests.linkLibC();
    exe_tests.linkSystemLibrary("rocksdb");

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run the tests");
    test_step.dependOn(&run_tests.step);
    test_step.dependOn(&run_exe_tests.step);
}
