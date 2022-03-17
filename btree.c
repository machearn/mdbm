#pragma clang diagnostic push
#pragma ide diagnostic ignored "misc-no-recursion"
//
// Created by Machearn Ning on 2/4/22.
//

#include "btree.h"

// todo: optimize write page to disk
Page* splitPage(int fd, Page* page);
int addInternalKey(int fd, Page* child, uint64_t key);

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

    return read(fd, page, sizeof(Page));
}

ssize_t dumpPage(int fd, Page* page) {
    off_t offset = page->offset;
    if (lseek(fd, offset, SEEK_SET) < 0) return -1;
    return write(fd, page, sizeof(Page));
}

ssize_t loadHeader(int fd, Header* header) {
    if (lseek(fd, 0, SEEK_SET) < 0) {
        return -1;
    }
    return read(fd, header, sizeof(Header));
}

ssize_t dumpHeader(int fd, Header* header) {
    if (lseek(fd, 0, SEEK_SET) < 0) return -1;
    return write(fd, header, sizeof(Header));
}

int initPage(Page* page, uint8_t isRoot, uint8_t type, off_t parent, off_t prev) {
    page->numCells = 0;
    page->isRoot = isRoot;
    page->parent = parent;
    page->nextPage = -1;
    page->prevPage = prev;
    page->type = type;
    page->leftMost = -1;
    page->offset = -1;
    return 0;
}

int addCell(Page* leaf, int pos, uint64_t key, off_t offset) {
    Cell* begin = leaf->cells;
    for (int i = leaf->numCells-1; i < pos; i--) {
        memcpy(begin + i + 1, begin+i, sizeof(Cell));
    }

    begin[pos+1].key = key;
    begin[pos+1].offset = offset;

    leaf->numCells++;

    return 0;
}

int deleteCell(int fd, Page* node, int pos) {
    if (node->numCells < 1) return -1;

    for (int i = pos; i < node->numCells-1; i++) {
        memcpy((node->cells)+i, (node->cells)+i+1, sizeof(Cell));
    }
    node->numCells--;
    if (dumpPage(fd, node) < 0) return -1;
    return 0;
}

int searchInternalNode(Page* node, uint64_t key) {
    uint8_t size = node->numCells;
    if (size <= 0) return -1;
    if (key < node->cells[0].key) return MAX_CELL;

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

Page* splitPage(int fd, Page* page) {
    Header header;
    loadHeader(fd, &header);

    Page* newPage = mallocPage();
    initPage(newPage, 0, page->type, page->parent, page->offset);

    uint8_t num = page->numCells;
    uint8_t half = num / 2;

    Cell* begin = page->cells;
    Cell* newBegin = newPage->cells;

    for (int i = half; i < num; i++) {
        memcpy(newBegin+i-half, begin+i, sizeof(Cell));
    }

    off_t off;
    if ((off = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&newPage);
        return NULL;
    }

    newPage->numCells = num - half;
    newPage->nextPage = page->nextPage;
    newPage->offset = off;

    page->numCells = half;
    page->isRoot = 0;
    page->nextPage = newPage->offset;

    dumpPage(fd, newPage);
    dumpPage(fd, page);
    header.nodeNumber++;

    if (page->isRoot) {
        Page* newRoot = mallocPage();
        initPage(newRoot, 1, INTERNAL_NODE, -1, -1);

        if ((off = lseek(fd, 0, SEEK_END)) < 0) {
            freePage(&newRoot);
            freePage(&newPage);
            return NULL;
        }
        newRoot->leftMost = page->offset;
        newRoot->numCells = 1;
        newRoot->offset = off;

        newRoot->cells->key = newPage->cells->key;
        newRoot->cells->offset = newPage->offset;
        header.nodeNumber++;
        header.height++;

        page->parent = off;
        newPage->parent = off;
        dumpPage(fd, newPage);
        dumpPage(fd, page);
        dumpPage(fd, newRoot);
        freePage(&newRoot);
    } else {
        addInternalKey(fd, page, newPage->cells->key);
    }

    if (dumpHeader(fd, &header) < 0) return NULL;
    return newPage;
}

// todo: when error occurred, undo all the write operation
int insertInternalPage(int fd, Page* prev, Page* child, uint64_t key) {
    Header header;
    loadHeader(fd, &header);

    Page* newPage = mallocPage();
    initPage(newPage, 0, INTERNAL_NODE, prev->parent, prev->offset);
    off_t off;
    if ((off = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&newPage);
        return -1;
    }
    newPage->cells->key = key;
    newPage->cells->offset = child->offset;
    newPage->offset = off;
    newPage->numCells = 1;
    prev->nextPage = off;
    if (dumpPage(fd, newPage) < 0) {
        freePage(&newPage);
        return -1;
    }
    header.nodeNumber++;

    child->parent = off;
    if (dumpPage(fd, child) < 0) {
        freePage(&newPage);
        return -1;
    }

    if (dumpPage(fd, prev) < 0) {
        freePage(&newPage);
        return -1;
    }

    if (prev->isRoot) {
        Page* newRoot = mallocPage();
        if ((off = lseek(fd, 0, SEEK_END)) < 0) {
            freePage(&newPage);
            freePage(&newRoot);
            return -1;
        }
        initPage(newRoot, 1, INTERNAL_NODE, -1, -1);

        prev->isRoot = 0;
        prev->parent = off;
        newPage->parent = off;

        newRoot->leftMost = prev->offset;
        newRoot->offset = off;
        newRoot->cells->key = key;
        newRoot->cells->offset = newPage->offset;

        if (dumpPage(fd, newRoot) < 0 ||
            dumpPage(fd, newPage) < 0 ||
            dumpPage(fd, prev) < 0) {
            freePage(&newPage);
            freePage(&newRoot);
            return -1;
        }

        header.rootOffset = off;
        freePage(&newRoot);
    } else {
        addInternalKey(fd, newPage, key);
    }

    lseek(fd, 0, SEEK_SET);
    write(fd, &header, sizeof(Header));

    freePage(&newPage);
    return 0;
}

int addInternalKey(int fd, Page* child, uint64_t key) {
    Page* node = mallocPage();
    off_t nodeOffset = child->parent;
    if (loadPage(fd, nodeOffset, node) < 0) return -1;
    int pos = searchInternalNode(node, key);

    if (pos < 0) return -1;
    if (pos == MAX_CELL) return -1;

    if (node->cells[pos].key == key) return -1;

    if (pos == MAX_CELL-1) return insertInternalPage(fd, node, child, key);

    if (node->numCells == MAX_CELL) {
        Page* newNode = splitPage(fd, node);
        int pos1 = searchInternalNode(node, key);
        int pos2 = searchInternalNode(newNode, key);

        if (pos2 == -1) {
            addCell(node, pos1, key, child->offset);
        } else {
            addCell(newNode, pos2, key, child->offset);
        }
        freePage(&node);
        return 0;
    }

    Cell* begin = node->cells;
    for (int i = node->numCells-1; i < pos; i--) {
        memcpy(begin+i+1, begin+i, sizeof(Cell));
    }
    addCell(node, pos, key, child->offset);

    return 0;
}

int insertLeafPage(int fd, Page* prev, uint64_t key, off_t offset) {
    Page* newLeaf = mallocPage();
    initPage(newLeaf, 0, LEAF_NODE, prev->parent, prev->offset);
    newLeaf->cells->key = key;
    newLeaf->cells->offset = offset;

    off_t off;
    if ((off = lseek(fd, 0, SEEK_END)) < 0) return -1;
    newLeaf->offset = off;
    prev->nextPage = off;
    dumpPage(fd, newLeaf);

    if (lseek(fd, prev->offset, SEEK_SET) < 0) return -1;
    dumpPage(fd, prev);

    addInternalKey(fd, newLeaf, key);
    return 0;
}

int createTree(const char* fileName) {
    Header header;
    header.magicNumber = 0x1234;
    header.orderNumber = MAX_CELL;
    header.height = 2;
    header.nodeNumber = 3;

    header.rootOffset = sizeof(Header);

    int fd = open(fileName, O_WRONLY);
    write(fd, &header, sizeof(Header));

    Page* root = mallocPage();
    initPage(root, 1, INTERNAL_NODE, -1, -1);
    if ((root->offset = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&root);
        return -1;
    }
    header.rootOffset = root->offset;
    dumpPage(fd, root);

    Page* leftLeaf = mallocPage();
    initPage(leftLeaf, 0, LEAF_NODE, root->offset, -1);
    if ((leftLeaf->offset = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&root);
        freePage(&leftLeaf);
        return -1;
    }
    dumpPage(fd, leftLeaf);
    root->leftMost = leftLeaf->offset;

    freePage(&leftLeaf);

    Page* rightLeaf = mallocPage();
    initPage(rightLeaf, 0, LEAF_NODE, root->offset, leftLeaf->offset);
    if ((rightLeaf->offset = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&root);
        freePage(&rightLeaf);
        return -1;
    }
    dumpPage(fd, rightLeaf);
    root->cells->key = MAX_CELL;
    root->cells->offset = rightLeaf->offset;
    root->numCells = 1;

    freePage(&rightLeaf);

    if (lseek(fd, root->offset, SEEK_SET) < 0) {
        freePage(&root);
        return -1;
    }
    dumpPage(fd, root);
    freePage(&root);

    header.mostLeftLeafOffset = leftLeaf->offset;
    if (lseek(fd, 0, SEEK_SET) < 0) {
        return -1;
    }
    write(fd, &header, sizeof(Header));
    return 0;
}

int search(int fd, Page* root, uint64_t key, Cell* cell) {
    Page* node = root;
    while (node->type == INTERNAL_NODE) {
        int pos = searchInternalNode(node, key);
        if (pos < 0) return -1;

        off_t offset;
        if (pos >= MAX_CELL) offset = node->leftMost;
        else offset = node->cells[pos].offset;

        if (loadPage(fd, offset, node) < 0) return -1;
    }

    return searchLeafNode(node, key, cell);
}

int insert(int fd, Page* root, uint64_t key, off_t offset) {
    int pos = search(fd, root, key, NULL);
    if (pos < 0) return -1;

    Page* leaf = root;
    if (leaf->cells[pos].key == key) return -1;

    if (pos == MAX_CELL-1) return insertLeafPage(fd, leaf, key, offset);

    if (leaf->numCells == MAX_CELL) {
        Page* newLeaf = splitPage(fd, leaf);
        int pos1 = searchLeafNode(leaf, key, NULL);
        int pos2 = searchLeafNode(newLeaf, key, NULL);

        if (pos2 == -1) {
            addCell(leaf, pos1, key, offset);
            dumpPage(fd, leaf);
        } else {
            addCell(newLeaf, pos2, key, offset);
            dumpPage(fd, newLeaf);
        }
        free(newLeaf);
        return 0;
    }

    addCell(leaf, pos, key, offset);
    dumpPage(fd, leaf);

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

#pragma clang diagnostic pop