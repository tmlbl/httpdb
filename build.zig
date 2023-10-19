const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "csvd",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });

    exe.linkLibC();
    exe.linkSystemLibraryName("rocksdb");
    exe.addIncludePath(.{ .path = "/usr/include" });

    exe.addModule("zinatra", b.createModule(.{
        .source_file = .{ .path = "./zinatra/src/App.zig" },
    }));

    b.installArtifact(exe);

    const tests = b.addTest(.{
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = .Debug,
    });

    const run_tests = b.addRunArtifact(tests);

    const test_step = b.step("test", "Run the tests");
    test_step.dependOn(&run_tests.step);
}
