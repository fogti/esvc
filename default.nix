{ pkgs ? import <nixpkgs> {} }:
let
  monokai-extended = pkgs.fetchFromGitHub {
    owner = "jonschlinkert";
    repo = "sublime-monokai-extended";
    rev = "0ca4e75291515c4d47e2d455e598e03e0dc53745";
    hash = "sha256-AmJJNkzIQUkGfrpOLy08kX3uYMmAlVq7mOjHcD3v2FE=";
    meta.license = pkgs.lib.licenses.mit;
  } + "/Monokai Extended.tmTheme";
  rust-stuff = import ./Cargo.nix {
    inherit pkgs;
    defaultCrateOverrides = pkgs.defaultCrateOverrides // {
      pyo3-build-config = attrs: {
        buildInputs = [ pkgs.python39 ];
      };
      exvc = attrs: {
        EXVC_DEFAULT_THEME = "Monokai Extended";
        EXVC_DFL_THEME_PATH = monokai-extended;
      };
    };
  };
  core-rust = rust-stuff.workspaceMembers.esvc-core.build.override {
    runTests = true;
  };
in
  rust-stuff.workspaceMembers.exvc.build.override {
    testInputs = [ core-rust ];
    runTests = true;
  }
