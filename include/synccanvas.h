#ifndef SYNCCANVAS_H
#define SYNCCANVAS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#pragma pack(push, 8)
typedef struct {
    int64_t lamport_ts;
    double pos_fractional;
    char char_val;
    int32_t is_deleted; // 0: Active, 1: Tombstone
    char client_id[32];
} CRDTChar;

typedef struct {
    int32_t length;
    char compiled_text[4096];
    int32_t total_tombstones;
    int32_t total_elements;
} CRDTDocumentView;
#pragma pack(pop)

// Merge set of CRDT character items deterministically into unified document text
void merge_crdt_characters(const CRDTChar* chars, int32_t count, CRDTDocumentView* out_view);

// Fractional position allocator between two boundary elements
double generate_fractional_position(double prev_pos, double next_pos);

// Lamport logical clock update
int64_t advance_lamport_clock(int64_t local_ts, int64_t remote_ts);

#ifdef __cplusplus
}
#endif

#endif // SYNCCANVAS_H
