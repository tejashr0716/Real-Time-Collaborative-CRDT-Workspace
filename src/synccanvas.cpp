#include "synccanvas.h"
#include <vector>
#include <algorithm>
#include <cstring>

struct CRDTCharComparator {
    bool operator()(const CRDTChar& a, const CRDTChar& b) const {
        if (a.pos_fractional != b.pos_fractional) {
            return a.pos_fractional < b.pos_fractional;
        }
        if (a.lamport_ts != b.lamport_ts) {
            return a.lamport_ts < b.lamport_ts;
        }
        return std::strncmp(a.client_id, b.client_id, sizeof(a.client_id)) < 0;
    }
};

extern "C" {

void merge_crdt_characters(const CRDTChar* chars, int32_t count, CRDTDocumentView* out_view) {
    if (!out_view) return;
    std::memset(out_view, 0, sizeof(CRDTDocumentView));

    if (!chars || count <= 0) return;

    std::vector<CRDTChar> char_list(chars, chars + count);
    std::sort(char_list.begin(), char_list.end(), CRDTCharComparator());

    int32_t text_idx = 0;
    int32_t tombstones = 0;

    for (const auto& ch : char_list) {
        if (ch.is_deleted) {
            tombstones++;
            continue;
        }
        if (text_idx < (int32_t)(sizeof(out_view->compiled_text) - 1)) {
            out_view->compiled_text[text_idx++] = ch.char_val;
        }
    }
    out_view->compiled_text[text_idx] = '\0';
    out_view->length = text_idx;
    out_view->total_tombstones = tombstones;
    out_view->total_elements = count;
}

double generate_fractional_position(double prev_pos, double next_pos) {
    if (prev_pos < 0.0) prev_pos = 0.0;
    if (next_pos <= 0.0) {
        return prev_pos + 1.0;
    }
    if (prev_pos >= next_pos) {
        return prev_pos + 0.001;
    }
    return prev_pos + ((next_pos - prev_pos) / 2.0);
}

int64_t advance_lamport_clock(int64_t local_ts, int64_t remote_ts) {
    return std::max(local_ts, remote_ts) + 1;
}

} // extern "C"
