{
  description = "HTTPDB dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.rocksdb
            pkgs.zig
            pkgs.pkg-config
            pkgs.clang
            pkgs.lldb # optional for debugging
          ];

          shellHook = ''
            export CFLAGS="-I${pkgs.rocksdb}/include"
            export LIBRARY_PATH="${pkgs.rocksdb}/lib"
            export LD_LIBRARY_PATH="${pkgs.rocksdb}/lib"
          '';
        };
      }
    );
}
