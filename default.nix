{ pkgs ? import <nixpkgs> {} }:
let
  rust-stuff = import ./Cargo.nix {
    inherit pkgs;
  };
  core-rust = rust-stuff.workspaceMembers.esvc-core.build.override {
    runTests = true;
  };
in
  rust-stuff.workspaceMembers.exvc.build.override {
    testInputs = [ core-rust ];
    runTests = true;
  }
