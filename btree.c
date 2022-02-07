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

int loadPage(int fd, off_t offset, Page* page) {
    if (lseek(fd, offset, SEEK_SET) < 0) {
        return -1;
    }

    if (read(fd, page, 4096) < 0) {
        return -1;
    }
    return 0;
}

off_t searchInternalNode(Page* node, uint64_t key) {
    uint8_t size = node->numCells;
    if (key < node->cells[0].key) return node->leftMost;
    if (key >= node->cells[size-1].key) return node->cells[size-1].offset;

    uint8_t left = 0;
    uint8_t right = size;

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
        if (node->cells[mid].key <= key) {
            left = mid+1;
        }
        else {
            right = mid;
        }
    }

    if (cell)
        memcpy((void*)((node->cells)+left-1), (void*)cell, sizeof(Cell));
    return left-1;
}

int search(Page* root, uint64_t key, Cell* cell) {
    if (!root->isRoot) return -1;
    int fd = root->fd;

    Page* node = root;
    while (node->type == INTERNAL_NODE) {
        off_t offset = searchInternalNode(node, key);
        if (loadPage(fd, offset, node) < 0) return -1;
    }

    return searchLeafNode(node, key, cell);
}

int insert(Page* root, uint64_t key, off_t offset) {
    int pos;
    pos = search(root, key, NULL);
    if (pos < 0) return -1;

    Page* leaf = root;
    if (leaf->cells[pos].key == key) return -1;

    if (pos == root->numCells-1) return addLeafPage(leaf, key, offset);

    Cell* begin = leaf->cells;
    for (int i = leaf->numCells-1; i < pos; i--) {
        memcpy(begin + i + 1, begin+i, sizeof(Cell));
        (begin+i+1)->nextCell = begin+i+2;
        (begin+i+1)->prevCell = begin+i;
    }

    begin[pos+1].key = key;
    begin[pos+1].offset = offset;
    begin[pos+1].prevCell = begin+pos;
    begin[pos+1].prevCell = begin+pos+2;

    begin[leaf->numCells].nextCell = NULL;
    leaf->numCells++;

    return 0;
}