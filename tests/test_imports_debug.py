"""Debug imports."""
import sys
from pathlib import Path

print("=" * 60)
print("DEBUGGING IMPORTS")
print("=" * 60)

# Show current directory
print(f"\nCurrent directory: {Path.cwd()}")

# Show Python path
print(f"\nPython path:")
for p in sys.path:
    print(f"  - {p}")

# Show where conftest.py is
conftest_path = Path(__file__).parent / "conftest.py"
print(f"\nconftest.py location: {conftest_path}")
print(f"conftest.py exists: {conftest_path.exists()}")

# Show where src should be
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
print(f"\nProject root: {project_root}")
print(f"Source path: {src_path}")
print(f"Source exists: {src_path.exists()}")

# Check for key files
key_files = [
    src_path / "stac_fastapi" / "__init__.py",
    src_path / "stac_fastapi" / "globus_search" / "__init__.py",
    src_path / "stac_fastapi" / "globus_search" / "app.py",
]

print(f"\nKey files:")
for f in key_files:
    print(f"  - {f.name}: {'✅' if f.exists() else '❌'} ({f})")

# Try import
print(f"\nTrying import...")
try:
    from stac_fastapi.globus_search import app
    print("✅ SUCCESS: Import worked!")
except ImportError as e:
    print(f"❌ FAILED: {e}")
    print("\nTrying to add src to path...")
    sys.path.insert(0, str(src_path))
    print(f"Added: {src_path}")
    try:
        from stac_fastapi.globus_search import app
        print("✅ SUCCESS after adding to path!")
    except ImportError as e2:
        print(f"❌ STILL FAILED: {e2}")

print("=" * 60)