import base64
from cryptography.hazmat.primitives.asymmetric import ed25519
from typing import Dict

class DeviceKeyRing:
    def __init__(self):
        self._private_keys: Dict[str, ed25519.Ed25519PrivateKey] = {}
        self._public_keys: Dict[str, ed25519.Ed25519PublicKey] = {}

    def register_device(self, device_id: str) -> str:
        if device_id in self._public_keys:
            raw_pub = self._public_keys[device_id].public_bytes_raw()
            return base64.b64encode(raw_pub).decode("utf-8")
        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        self._private_keys[device_id] = private_key
        self._public_keys[device_id] = public_key
        raw_pub = public_key.public_bytes_raw()
        return base64.b64encode(raw_pub).decode("utf-8")

    def sign(self, device_id: str, message: bytes) -> str:
        if device_id not in self._private_keys:
            self.register_device(device_id)
        sig = self._private_keys[device_id].sign(message)
        return base64.b64encode(sig).decode("utf-8")

    def verify(self, device_id: str, message: bytes, signature_b64: str) -> bool:
        if device_id not in self._public_keys:
            return False
        try:
            raw_sig = base64.b64decode(signature_b64)
            self._public_keys[device_id].verify(raw_sig, message)
            return True
        except Exception:
            return False

# Exported singleton instance
GLOBAL_KEYRING = DeviceKeyRing()

__all__ = ["DeviceKeyRing", "GLOBAL_KEYRING"]