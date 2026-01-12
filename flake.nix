{
  description = "GitHub PR merge train bot";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      overlays = [(import rust-overlay)];
      pkgs = import nixpkgs {
        inherit system overlays;
        config.allowUnfree = true;
      };

      rustToolchain = pkgs.rust-bin.stable.latest.default.override {
        extensions = ["rust-src" "clippy" "rust-analyzer" "rustfmt"];
      };

      merge-train-bot = pkgs.rustPlatform.buildRustPackage {
        pname = "merge-train-bot";
        version = "0.1.0";

        src = ./.;

        cargoLock = {
          lockFile = ./Cargo.lock;
        };

        nativeBuildInputs = with pkgs; [
          pkg-config
          git # needed for property tests that create git repos
        ];

        buildInputs = with pkgs; [
          openssl
          libiconv
        ];

        # Pass git revision to the build
        ROBOCOP_GIT_HASH =
          if (self ? rev) && (self.rev != null)
          then self.rev
          else "dirty";

        meta = with pkgs.lib; {
          description = "GitHub merge train bot";
          homepage = "https://github.com/Smaug123/merge-train";
          license = licenses.mit;
          maintainers = [];
        };
      };
    in {
      packages = {
        default = merge-train-bot;
      };

      devShells.default = pkgs.mkShell {
        buildInputs =
          [
            rustToolchain
            pkgs.pkg-config
            pkgs.openssl
            pkgs.libiconv
            pkgs.git
            pkgs.claude-code
            pkgs.codex
            pkgs.alejandra
            pkgs.shellcheck
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
            pkgs.bubblewrap
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.apple-sdk
          ];

        RUST_BACKTRACE = "1";
      };
    });
}
