{
  description = "HTTPDB dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";

    zig-overlay.url = "github:mitchellh/zig-overlay";
    zig-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      zig-overlay,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        zig = zig-overlay.packages.${system}."0.15.1";
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.rocksdb
            zig
            pkgs.pkg-config
            pkgs.clang
            pkgs.glibc
            pkgs.lldb
            pkgs.bun  # for running tests / examples
          ];

          shellHook = ''
            export C_INCLUDE_PATH="-I${pkgs.rocksdb}/include:${pkgs.glibc.dev}/include"
            export LIBRARY_PATH="${pkgs.rocksdb}/lib"
            export LD_LIBRARY_PATH="${pkgs.rocksdb}/lib"
          '';
        };
      }
    );
}
