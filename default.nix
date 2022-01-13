{ pkgs ? import <nixpkgs> {} }:
let
  my-python = pkgs.python39;
  rust-stuff = import ./Cargo.nix {
    inherit pkgs;
    defaultCrateOverrides = pkgs.defaultCrateOverrides // {
      pyo3-build-config = attrs: {
        buildInputs = [ my-python ];
      };
    };
  };
  my-python-ver = my-python.pythonVersion;
  name-indra = "esvc_indra";
  core-rust = rust-stuff.workspaceMembers.esvc-core.build.override {
    runTests = true;
  };
  indra-rust = rust-stuff.workspaceMembers.esvc-indra.build;
  indra-pypkg = pkgs.stdenvNoCC.mkDerivation {
    name = "python${my-python-ver}-${name-indra}";
    src = indra-rust.lib + "/lib";
    # this is just here to run the core tests
    honey = core-rust;
    buildPhase = ''
      runHook preBuild
      runHook postBuild
    '';
    installPhase = ''
      DEST="$out/lib/python${my-python-ver}/site-packages/${name-indra}"
      install -D -T \
        "$src/lib${name-indra}.so" \
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
