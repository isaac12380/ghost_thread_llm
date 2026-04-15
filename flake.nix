{
  description = "Nix dev shell for the Ghost Threading artifact toolchain";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = with pkgs; [
          gcc13
          clang
          binutils
          llvmPackages.openmp
          gnumake
          python3
          gnuplot
          coreutils
          findutils
          gnugrep
        ];

        shellHook = ''
          export CC=gcc
          export CXX=g++
          export AS=as
          export LD=ld

          echo "Ghost Threading toolchain shell"
          echo "  gcc:   $(gcc --version | head -n1)"
          echo "  g++:   $(g++ --version | head -n1)"
          echo "  clang: $(clang --version | head -n1)"
          echo "  as:    $(as --version | head -n1)"
          echo
          echo "Build and plotting should run inside this shell."
          echo "Runtime tools such as perf, taskset, and membw still come from the host."
          echo "If flakes are not enabled globally, start with:"
          echo '  NIX_CONFIG="experimental-features = nix-command flakes" nix develop'
          echo "If this checkout is not tracked by Git yet, use:"
          echo '  NIX_CONFIG="experimental-features = nix-command flakes" nix develop path:.'
        '';
      };
    };
}
