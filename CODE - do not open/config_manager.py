"""
Config encryption manager for CNA Web App.

Encrypts config.py for safe storage in the git repo using Fernet
symmetric encryption. The encryption key is shared via network drive.

Usage:
    python config_manager.py encrypt        # config.py -> config.enc
    python config_manager.py decrypt        # config.enc -> config.py
    python config_manager.py generate-key   # Create a new encryption key
"""

from pathlib import Path
import sys

from cryptography.fernet import Fernet

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
CONFIG_PY = ROOT_DIR / "config.py"
CONFIG_ENC = APP_DIR / "config.enc"
KEY_FILE = APP_DIR / "config.key"
NETWORK_KEY = Path(
    r"\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.key"
)


def _load_key() -> bytes:
    """Load encryption key from local file, falling back to network share."""
    if KEY_FILE.exists():
        return KEY_FILE.read_bytes().strip()
    if NETWORK_KEY.exists():
        key = NETWORK_KEY.read_bytes().strip()
        KEY_FILE.write_bytes(key)
        return key
    print("ERROR: Encryption key not found.")
    print(f"  Local:   {KEY_FILE}")
    print(f"  Network: {NETWORK_KEY}")
    sys.exit(1)


def generate_key():
    """Generate a new Fernet key and save locally + to network share."""
    key = Fernet.generate_key()
    KEY_FILE.write_bytes(key)
    print(f"Key saved to: {KEY_FILE}")
    try:
        NETWORK_KEY.write_bytes(key)
        print(f"Key saved to: {NETWORK_KEY}")
    except Exception as e:
        print(f"WARNING: Could not save to network share: {e}")
        print(f"Please manually copy {KEY_FILE} to {NETWORK_KEY}")


def encrypt():
    """Encrypt config.py -> config.enc."""
    if not CONFIG_PY.exists():
        print(f"ERROR: {CONFIG_PY} not found.")
        sys.exit(1)
    key = _load_key()
    f = Fernet(key)
    plaintext = CONFIG_PY.read_bytes()
    ciphertext = f.encrypt(plaintext)
    CONFIG_ENC.write_bytes(ciphertext)
    print(f"Encrypted: config.py -> config.enc")


def decrypt():
    """Decrypt config.enc -> config.py."""
    if not CONFIG_ENC.exists():
        print(f"ERROR: {CONFIG_ENC} not found.")
        sys.exit(1)
    key = _load_key()
    f = Fernet(key)
    ciphertext = CONFIG_ENC.read_bytes()
    try:
        plaintext = f.decrypt(ciphertext)
    except Exception:
        print("ERROR: Decryption failed. The key may be incorrect.")
        sys.exit(1)
    CONFIG_PY.write_bytes(plaintext)
    print(f"Decrypted: config.enc -> config.py")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python config_manager.py [encrypt|decrypt|generate-key]")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "encrypt":
        encrypt()
    elif cmd == "decrypt":
        decrypt()
    elif cmd == "generate-key":
        generate_key()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
