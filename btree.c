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

ssize_t loadPage(int fd, off_t offset, Page* page) {
    if (lseek(fd, offset, SEEK_SET) < 0) {
        return -1;
    }

    return read(fd, page, 4096);
}

ssize_t dumpPage(int fd, Page* page) {
    off_t offset = page->offset;
    if (lseek(fd, offset, SEEK_SET) < 0) return -1;
    return write(fd, page, sizeof(Page));
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

int searchInternalNode(Page* node, uint64_t key) {
    if (node->numCells <= 0) return -1;
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

    return (int)left-1;
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
    return (int)left-1;
}

Page* splitNode(int fd, Page* node) {
    Page* newNode = mallocPage();

    uint8_t num = node->numCells;
    uint8_t half = num / 2;

    Cell* begin = node->cells;
    Cell* newBegin = newNode->cells;

    for (int i = 0; i < half; i++) {
        memcpy(newBegin+i, begin+half+i, sizeof(Cell));
    }

    newNode->numCells = num - half;
    newNode->nextPage = node->nextPage;
    newNode->prevPage = node->offset;
    newNode->type = node->type;
    newNode->leftMost = -1;
    newNode->isRoot = 0;

    off_t off;
    if ((off = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&newNode);
        return NULL;
    }

    if (node->isRoot) {
        node->isRoot = 0;
        node->parent = 0;
        node->offset = off;
        node->numCells = half;
        write(fd, node, sizeof(Page));

        if ((off = lseek(fd, 0, SEEK_END)) < 0) {
            freePage(&newNode);
            return NULL;
        }
        newNode->parent = 0;
        newNode->prevPage = node->offset;
        write(fd, newNode, sizeof(Page));

        node->nextPage = off;
        if (lseek(fd, node->offset, SEEK_SET) < 0) {
            freePage(&newNode);
            return NULL;
        }
        write(fd, node, sizeof(Page));

        Page* root = mallocPage();
        root->numCells = 1;
        root->cells->key = newNode->cells->key;
        root->cells->offset = off;
        root->offset = 0;
        root->parent = -1;
        root->nextPage = -1;
        root->prevPage = -1;
        root->isRoot = 1;
        root->type = INTERNAL_NODE;
        root->leftMost = node->offset;
        if (lseek(fd, 0, SEEK_SET) < 0) {
            freePage(&newNode);
            return NULL;
        }
        write(fd, root, sizeof(Page));

    } else {
        addInternalKey(fd, node->parent, newNode->cells->key, off);
        newNode->parent = node->parent;
        newNode->offset = off;
        write(fd, newNode, sizeof(Page));
        node->numCells = half;
        node->nextPage = off;
        write(fd, node, sizeof(Page));
    }

    return newNode;
}

// todo: when error occurred, undo all the write operation
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
        addInternalKey(fd, newPage->parent, key, off);
    }

    freePage(&newPage);
    return 0;
}

int addInternalKey(int fd, off_t nodeOff, uint64_t key, off_t offset) {
    Page* node = mallocPage();
    if (loadPage(fd, nodeOff, node) < 0) return -1;
    int pos = searchInternalNode(node, key);

    if (pos < 0) return -1;
    if (node->cells[pos].key == key) return -1;

    if (pos == MAX_CELL-1) return insertInternalPage(fd, node, key, offset);

    if (node->numCells == MAX_CELL) {
        Page* newNode = splitNode(fd, node);
        int pos1 = searchInternalNode(node, key);
        int pos2 = searchInternalNode(newNode, key);

        if (pos2 == -1) {
            addCell(node, pos1, key, offset);
        } else {
            addCell(newNode, pos2, key, offset);
        }
        freePage(&node);
        return 0;
    }

    Cell* begin = node->cells;
    for (int i = node->numCells-1; i < pos; i--) {
        memcpy(begin+i+1, begin+i, sizeof(Cell));
        (begin+i+1)->nextCell = begin+i+2;
        (begin+i+1)->prevCell = begin+i;
    }
    addCell(node, pos, key, offset);

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

    addInternalKey(fd, newLeaf->parent, key, off);
    dumpPage(fd, newLeaf);
    return 0;
}

int search(int fd, Page* root, uint64_t key, Cell* cell) {
    Page* node = root;
    while (node->type == INTERNAL_NODE) {
        int pos = searchInternalNode(node, key);
        if (pos < 0) return -1;
        off_t offset = node->cells[pos].offset;
        if (loadPage(fd, offset, node) < 0) return -1;
    }

    return searchLeafNode(node, key, cell);
}

int addCell(Page* leaf, int pos, uint64_t key, off_t offset) {
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

int insert(int fd, Page* root, uint64_t key, off_t offset) {
    int pos = search(fd, root, key, NULL);;
    if (pos < 0) return -1;

    Page* leaf = root;
    if (leaf->cells[pos].key == key) return -1;

    if (pos == MAX_CELL-1) return insertLeafPage(fd, leaf, key, offset);

    if (leaf->numCells == MAX_CELL) {
        Page* newLeaf = splitNode(fd, leaf);
        int pos1 = searchLeafNode(leaf, key, NULL);
        int pos2 = searchLeafNode(newLeaf, key, NULL);

        if (pos2 == -1) {
            addCell(leaf, pos1, key, offset);
        } else {
            addCell(newLeaf, pos2, key, offset);
        }
        dumpPage(fd, newLeaf);
        free(newLeaf);
        return 0;
    }

    Cell* begin = leaf->cells;
    for (int i = leaf->numCells-1; i < pos; i--) {
        memcpy(begin + i + 1, begin+i, sizeof(Cell));
        (begin+i+1)->nextCell = begin+i+2;
        (begin+i+1)->prevCell = begin+i;
    }
    addCell(leaf, pos, key, offset);
    dumpPage(fd, leaf);

    return 0;
}

int deleteCell(int fd, Page* node, int pos) {
    if (node->numCells < 1) return -1;

    memset((node->cells)+pos, 0, sizeof(Cell));
    node->numCells--;
    if (dumpPage(fd, node) < 0) return -1;
    return 0;
}

int delete(int fd, Page* root, uint64_t key) {
    int pos = search(fd, root, key, NULL);
    if (pos < 0) return -1;

    Page* leaf = root;
    if (leaf->cells[pos].key != key) return -1;

    return deleteCell(fd, leaf, pos);
}

int update(int fd, Page* root, uint64_t key, Cell* cell) {
    int pos = search(fd, root, key, NULL);
    if (pos < 0) return -1;

    Page* leaf = root;
    if (leaf->cells[pos].key != key) return -1;
    memcpy((leaf->cells)+pos, cell, sizeof(Cell));
    return 0;
}
