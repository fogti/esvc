{ pkgs ? import <nixpkgs> {} }:
let
  my-python = pkgs.python39;
  rust-stuff = import ./Cargo.nix {
    inherit pkgs;
    buildRustCrateForPkgs = pkgs: pkgs.buildRustCrate.override {
      defaultCrateOverrides = pkgs.defaultCrateOverrides // {
        pyo3-build-config = attrs: {
          buildInputs = [ my-python ];
        };
      };
    };
  };
  my-python-ver = my-python.pythonVersion;
  name-indra = "esvc_indra";
  indra-pypkg = pkgs.stdenvNoCC.mkDerivation {
    name = "python${my-python-ver}-${name-indra}";

    src = rust-stuff.workspaceMembers.esvc-indra.build.lib;
    buildPhase = ''
      runHook preBuild
      runHook postBuild
    '';
    installPhase = ''
      DEST="$out/lib/python${my-python-ver}/site-packages/${name-indra}"
      install -D -T \
        "$src/lib/lib${name-indra}.so" \
        "$DEST.so"
      mkdir -p "$DEST.dist-info"
      echo "${name-indra}" > "$DEST.dist-info/namespace_packages.txt"
      echo "${name-indra}" > "$DEST.dist-info/top_level.txt"
    '';

    passthru.pythonModule = my-python;
  };
in
  my-python.withPackages (p: [
    indra-pypkg
  ])

