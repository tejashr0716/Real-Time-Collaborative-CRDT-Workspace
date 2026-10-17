#include "synccanvas.h"
#include <iostream>
#include <chrono>
#include <vector>
#include <iomanip>
#include <cstring>

int main() {
    std::cout << "========================================================\n";
    std::cout << "   SyncCanvas C++ CRDT Sequence Merge Benchmark        \n";
    std::cout << "========================================================\n\n";

    const int DOC_SIZE = 500;
    std::vector<CRDTChar> chars(DOC_SIZE);
    for (int i = 0; i < DOC_SIZE; ++i) {
        chars[i].lamport_ts = i + 1;
        chars[i].pos_fractional = (double)(i + 1) * 0.1;
        chars[i].char_val = 'a' + (i % 26);
        chars[i].is_deleted = (i % 10 == 0) ? 1 : 0; // 10% tombstones
        std::snprintf(chars[i].client_id, sizeof(chars[i].client_id), "CLIENT_%02d", i % 5);
    }

    const int MERGE_RUNS = 100000;
    CRDTDocumentView view;

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < MERGE_RUNS; ++i) {
        merge_crdt_characters(chars.data(), DOC_SIZE, &view);
    }
    auto end = std::chrono::high_resolution_clock::now();
    double merge_sec = std::chrono::duration<double>(end - start).count();
    double char_ops = (double)(MERGE_RUNS * DOC_SIZE) / merge_sec;
    double merge_runs_sec = (double)MERGE_RUNS / merge_sec;

    std::cout << ">> CRDT Operations Merged:       " << std::fixed << std::setprecision(2)
              << (char_ops / 1e6) << " Million chars/sec\n";
    std::cout << ">> Full Document Merges:         " << std::fixed << std::setprecision(2)
              << merge_runs_sec << " merges/sec ("
              << (merge_sec * 1000.0 / MERGE_RUNS) << " ms/doc)\n";

    // Fractional Position Benchmark
    const int POS_RUNS = 10000000;
    double dummy_pos = 0.0;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < POS_RUNS; ++i) {
        dummy_pos += generate_fractional_position((double)i * 0.1, (double)(i + 1) * 0.1);
    }
    end = std::chrono::high_resolution_clock::now();
    double pos_sec = std::chrono::duration<double>(end - start).count();
    double pos_ops = (double)POS_RUNS / pos_sec;
    double pos_ns = (pos_sec / (double)POS_RUNS) * 1e9;

    std::cout << ">> Fractional Position Alloc:    " << std::fixed << std::setprecision(2)
              << (pos_ops / 1e6) << " Million allocs/sec ("
              << pos_ns << " ns/op)\n";

    std::cout << "\nSample Merged Document Content:\n";
    std::cout << "  Length:     " << view.length << " chars\n";
    std::cout << "  Tombstones: " << view.total_tombstones << "\n";
    std::cout << "  Preview:    \"";
    for (int i = 0; i < 40 && i < view.length; ++i) std::cout << view.compiled_text[i];
    std::cout << "...\"\n";
    std::cout << "========================================================\n";

    return 0;
}
