import sys
import importlib
from pathlib import Path

REQ_FILE = Path("requirements.txt")

# Canonical normalized aliases
IMPORT_ALIASES = {
    "python_dotenv": "dotenv",
    "smartapi_python": "SmartApi",
    "websocket_client": "websocket",
    "logzero": "logzero",
}

def normalize(pkg):
    return pkg.strip().lower().replace("-", "_")

def main():
    if not REQ_FILE.exists():
        print("❌ requirements.txt not found")
        sys.exit(1)

    failed = []

    with open(REQ_FILE) as f:
        packages = [
            normalize(line)
            for line in f
            if line.strip() and not line.startswith("#")
        ]

    print("\n📦 Verifying installed packages...\n")

    for pkg in packages:
        module = IMPORT_ALIASES.get(pkg, pkg)
        try:
            importlib.import_module(module)
            print(f"✅ {pkg.replace('_','-')} -> OK")
        except Exception as e:
            print(f"❌ {pkg.replace('_','-')} -> FAILED ({e})")
            failed.append(pkg)

    if failed:
        print("\n❌ Installation verification failed for:")
        for f in failed:
            print(f"   - {f.replace('_','-')}")
        sys.exit(1)

    print("\n🎉 All packages installed and importable successfully!")

if __name__ == "__main__":
    main()
