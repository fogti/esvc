{ pkgs ? import <nixpkgs> {} }:
let
  rust-stuff = import ./Cargo.nix {
    inherit pkgs;
    buildRustCrateForPkgs = pkgs: pkgs.buildRustCrate.override {
      defaultCrateOverrides = pkgs.defaultCrateOverrides // {
        pyo3-build-config = attrs: {
          buildInputs = [ pkgs.python39 ];
        };
      };
    };
  };
in
  rust-stuff.workspaceMembers.esvc-indra.build
