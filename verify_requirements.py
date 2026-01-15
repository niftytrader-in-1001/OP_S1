import sys
import importlib
from pathlib import Path

REQ_FILE = Path("requirements.txt")

# Some packages have different import names
IMPORT_ALIASES = {
    "python-dotenv": "dotenv",
    "smartapi-python": "SmartApi",
    "logzero": "logzero"
}

def normalize(pkg):
    return pkg.replace("-", "_").lower()

def main():
    if not REQ_FILE.exists():
        print("❌ requirements.txt not found")
        sys.exit(1)

    failed = []
    packages = []

    with open(REQ_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            pkg = line.split("==")[0]
            packages.append(pkg)

    print("\n📦 Verifying installed packages...\n")

    for pkg in packages:
        module = IMPORT_ALIASES.get(pkg, normalize(pkg))
        try:
            importlib.import_module(module)
            print(f"✅ {pkg} -> OK")
        except Exception as e:
            print(f"❌ {pkg} -> FAILED ({e})")
            failed.append(pkg)

    if failed:
        print("\n❌ Installation verification failed for:")
        for f in failed:
            print(f"   - {f}")
        sys.exit(1)

    print("\n🎉 All packages installed and importable successfully!")

if __name__ == "__main__":
    main()
