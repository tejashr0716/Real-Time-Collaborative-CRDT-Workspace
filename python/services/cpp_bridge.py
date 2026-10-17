import ctypes
import os
from typing import List, Dict, Any, Optional

class CCRDTChar(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("lamport_ts", ctypes.c_int64),
        ("pos_fractional", ctypes.c_double),
        ("char_val", ctypes.c_char),
        ("is_deleted", ctypes.c_int32),
        ("client_id", ctypes.c_char * 32)
    ]

class CCRDTDocumentView(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("length", ctypes.c_int32),
        ("compiled_text", ctypes.c_char * 4096),
        ("total_tombstones", ctypes.c_int32),
        ("total_elements", ctypes.c_int32)
    ]

class SyncCanvasEngine:
    def __init__(self, dylib_path: Optional[str] = None):
        if dylib_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            dylib_path = os.path.join(base_dir, "libsynccanvas.dylib")

        if not os.path.exists(dylib_path):
            raise FileNotFoundError(f"Shared library not found at: {dylib_path}")

        self.lib = ctypes.CDLL(dylib_path)

        # void merge_crdt_characters(const CRDTChar* chars, int32_t count, CRDTDocumentView* out_view);
        self.lib.merge_crdt_characters.argtypes = [
            ctypes.POINTER(CCRDTChar),
            ctypes.c_int32,
            ctypes.POINTER(CCRDTDocumentView)
        ]
        self.lib.merge_crdt_characters.restype = None

        # double generate_fractional_position(double prev_pos, double next_pos);
        self.lib.generate_fractional_position.argtypes = [ctypes.c_double, ctypes.c_double]
        self.lib.generate_fractional_position.restype = ctypes.c_double

        # int64_t advance_lamport_clock(int64_t local_ts, int64_t remote_ts);
        self.lib.advance_lamport_clock.argtypes = [ctypes.c_int64, ctypes.c_int64]
        self.lib.advance_lamport_clock.restype = ctypes.c_int64

    def merge_characters(self, char_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        count = len(char_list)
        if count == 0:
            return {"length": 0, "compiled_text": "", "total_tombstones": 0, "total_elements": 0}

        chars_arr = (CCRDTChar * count)()
        for i, ch in enumerate(char_list):
            char_str = str(ch.get("char_val", " "))
            c_char = char_str.encode("utf-8")[0] if char_str else ord(" ")
            client = str(ch.get("client_id", "ANON"))

            chars_arr[i] = CCRDTChar(
                lamport_ts=int(ch.get("lamport_ts", 1)),
                pos_fractional=float(ch.get("pos_fractional", (i + 1) * 0.1)),
                char_val=c_char,
                is_deleted=1 if ch.get("is_deleted") else 0,
                client_id=client.encode("utf-8")
            )

        view = CCRDTDocumentView()
        self.lib.merge_crdt_characters(chars_arr, ctypes.c_int32(count), ctypes.byref(view))

        return {
            "length": view.length,
            "compiled_text": view.compiled_text.decode("utf-8", errors="replace"),
            "total_tombstones": view.total_tombstones,
            "total_elements": view.total_elements
        }

    def allocate_position(self, prev_pos: float, next_pos: float) -> float:
        return round(self.lib.generate_fractional_position(ctypes.c_double(prev_pos), ctypes.c_double(next_pos)), 6)

    def advance_clock(self, local_ts: int, remote_ts: int) -> int:
        return self.lib.advance_lamport_clock(ctypes.c_int64(local_ts), ctypes.c_int64(remote_ts))
