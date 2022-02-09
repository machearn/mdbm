//
// Created by Machearn Ning on 2/4/22.
//

#include "btree.h"

Page* mallocPage() {
    Page* p;
    p = (Page*) malloc(sizeof(Page));
    memset(p, 0, sizeof(Page));
    return p;
}

int freePage(Page** page) {
    free(*page);
    *page = NULL;
    return 0;
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

int initPage(Page* page, uint8_t isRoot, uint8_t type, off_t parent, off_t prev, uint64_t key, off_t offset) {
    page->numCells = 1;
    page->isRoot = isRoot;
    page->parent = parent;
    page->nextPage = -1;
    page->prevPage = prev;
    page->type = type;
    page->leftMost = -1;

    page->cells->key = key;
    page->cells->offset = offset;
    page->cells->nextCell = page->cells;
    page->cells->prevCell = page->cells;

    page->offset = -1;
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

// todo: 在错误处理中，还原之前的写入操作
int insertInternalPage(int fd, Page* prev, uint64_t key, off_t offset) {
    Page* newPage = mallocPage();
    initPage(newPage, 0, INTERNAL_NODE, prev->parent, prev->offset, key, offset);
    off_t off;
    if ((off = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&newPage);
        return -1;
    }
    newPage->offset = off;
    prev->nextPage = off;
    write(fd, newPage, sizeof(Page));

    if (lseek(fd, prev->offset, SEEK_SET) < 0) {
        freePage(&newPage);
        return -1;
    }
    write(fd, prev, sizeof(Page));

    if (prev->isRoot) {
        Page* left = mallocPage();
        if (loadPage(fd, 0, left) < 0) {
            freePage(&left);
            return -1;
        }
        off_t leftMost;
        if ((leftMost=lseek(fd, 0, SEEK_END)) < 0) {
            freePage(&left);
            return -1;
        }
        write(fd, left, sizeof(Page));
        freePage(&left);

        Page* newRoot = mallocPage();
        initPage(newRoot, 1, INTERNAL_NODE, -1, -1, key, off);
        if (lseek(fd, 0, SEEK_SET) < 0) {
            freePage(&newRoot);
            return -1;
        }
        write(fd, newRoot, sizeof(Page));
        freePage(&newRoot);
        freePage(&newPage);

        return 0;
    } else {
        addKey(fd, newPage->parent, key, off);
    }

    freePage(&newPage);
    return 0;
}

int addKey(int fd, off_t nodeOff, uint64_t key, off_t offset) {
    Page* node = mallocPage();
    if (loadPage(fd, nodeOff, node) < 0) return -1;

    if (node->numCells < MAX_CELL) {
        node->cells[node->numCells].key = key;
        node->cells[node->numCells].offset = offset;
        node->cells[(node->numCells)-1].nextCell = node->cells+node->numCells;
        node->cells[node->numCells].prevCell = node->cells+node->numCells-1;
        node->cells[node->numCells].nextCell = node->cells;
        node->numCells++;
    } else {
        insertInternalPage(fd, node, key, offset);
    }
    return 0;
}

int insertLeafPage(int fd, Page* prev, uint64_t key, off_t offset) {
    Page* newLeaf = mallocPage();
    initPage(newLeaf, 0, LEAF_NODE, prev->parent, prev->offset, key, offset);

    off_t off;
    if ((off = lseek(fd, 0, SEEK_END)) < 0) return -1;
    newLeaf->offset = off;
    prev->nextPage = off;
    write(fd, newLeaf, sizeof(Page));

    if (lseek(fd, prev->offset, SEEK_SET) < 0) return -1;
    write(fd, prev, sizeof(Page));

    addKey(fd, newLeaf->parent, key, off);
    return 0;
}

int search(int fd, Page* root, uint64_t key, Cell* cell) {
    if (!root->isRoot) return -1;

    Page* node = root;
    while (node->type == INTERNAL_NODE) {
        off_t offset = searchInternalNode(node, key);
        if (loadPage(fd, offset, node) < 0) return -1;
    }

    return searchLeafNode(node, key, cell);
}

int insert(int fd, Page* root, uint64_t key, off_t offset) {
    int pos;
    pos = search(fd, root, key, NULL);
    if (pos < 0) return -1;

    Page* leaf = root;
    if (leaf->cells[pos].key == key) return -1;

    if (pos == root->numCells-1) return insertLeafPage(fd, leaf, key, offset);

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

    begin[leaf->numCells].nextCell = begin;
    begin->prevCell = begin+leaf->numCells;
    leaf->numCells++;

    return 0;
}