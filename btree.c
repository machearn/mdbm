//
// Created by Machearn Ning on 2/4/22.
//

#include "btree.h"

Page* mallocPage() {
    Page* p;
    p = (Page*) malloc(4096);
    memset(p, 0, 4096);
    return p;
}

int loadPage(int fd, off_t offset, Page** pPage) {
    if (lseek(fd, offset, SEEK_SET) < 0) {
        return -1;
    }
    if (read(fd, *pPage, 4096) < 0) {
        return -1;
    }
    return 0;
}

off_t searchInternalNode(Page* node, uint64_t key) {
    uint8_t size = node->numCells;
    uint8_t left = 0;
    uint8_t right = size;

    if (key < node->cells[left].key) return node->leftMost;
    if (key >= node->cells[right].key) return node->cells[size-1].offset;

    while (left < right) {
        uint8_t mid = left + (right - left) / 2;
        if (node->cells[mid].key <= key) {
            left = mid+1;
        }
        else {
            right = mid;
        }
    }

    return node->cells[left-1].offset;
}

int searchLeafNode(Page* node, uint64_t key, Cell* cell) {
    uint8_t size = node->numCells;
    uint8_t left = 0;
    uint8_t right = size;

    while (left < right) {
        uint8_t mid = left + (right - left) / 2;
        if (node->cells[mid].key == key) {
            memcpy((void*)((node->cells)+mid), (void*)cell, sizeof(Cell));
            return 0;
        }
        else if (node->cells[mid].key > key) {
            right = mid;
        }
        else {
            left = mid+1;
        }
    }

    return -1;
}
