#pragma clang diagnostic push
#pragma ide diagnostic ignored "misc-no-recursion"
//
// Created by Machearn Ning on 2/4/22.
//

#include "btree.h"

Page* splitPage(int fd, Header* header, Page* page);

int addInternalKey(int fd, Header* header, Page* child, uint64_t key);

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
    off_t off;
    if ((off = lseek(fd, offset, SEEK_SET)) < 0) return -1;
    if (readLockWait(fd, off, SEEK_SET, sizeof(Page)) < 0) return -1;
    ssize_t ret = read(fd, page, sizeof(Page));
    if (unlock(fd, off, SEEK_SET, sizeof(Page)) < 0) return -1;
    return ret;
}

ssize_t dumpPage(int fd, Page* page) {
    if (!page) return 0;

    off_t offset = page->offset;
    if (lseek(fd, offset, SEEK_SET) < 0) return -1;
    if (writeLockWait(fd, offset, SEEK_SET, sizeof(Page)) < 0) return -1;
    ssize_t ret = write(fd, page, sizeof(Page));
    if (unlock(fd, offset, SEEK_SET, sizeof(Page)) < 0) return -1;
    return ret;
}

ssize_t loadHeader(const char* fileName, Header* header) {
    int fd;
    if ((fd = open(fileName, O_RDWR)) < 0) return -1;
    if (readLockWait(fd, 0, SEEK_SET, sizeof(Header)) < 0) return -1;
    ssize_t ret = read(fd, header, sizeof(Header));
    if (unlock(fd, 0, SEEK_SET, sizeof(Header)) < 0) return -1;
    return ret;
}

ssize_t dumpHeader(int fd, Header* header) {
    if (lseek(fd, 0, SEEK_SET) < 0) return -1;
    if (writeLockWait(fd, 0, SEEK_SET, sizeof(Header)) < 0) return -1;
    ssize_t ret = write(fd, header, sizeof(Header));
    if (unlock(fd, 0, SEEK_SET, sizeof(Header)) < 0) return -1;
    return ret;
}

int initPage(Page* page, uint8_t isRoot, uint8_t type, off_t parent, off_t prev, off_t next,
             off_t offset, off_t leftMost) {
    page->numCells = 0;
    page->isRoot = isRoot;
    page->parent = parent;
    page->nextPage = next;
    page->prevPage = prev;
    page->type = type;
    page->leftMost = leftMost;
    page->offset = offset;
    return 0;
}

int addCell(Page* leaf, int pos, uint64_t key, off_t offset, size_t size) {
    Cell* begin = leaf->cells;
    for (int i = leaf->numCells - 1; i > pos; i--) {
        memcpy(begin + i + 1, begin + i, sizeof(Cell));
    }

    begin[pos + 1].key = key;
    begin[pos + 1].offset = offset;
    begin[pos + 1].size = size;

    leaf->numCells++;

    return 0;
}

int deleteCell(int fd, Page* node, int pos, Cell* cell) {
    if (node->numCells < 1) return -1;
    memcpy(cell, node->cells + pos, sizeof(Cell));

    for (int i = pos; i < node->numCells - 1; i++) {
        memcpy((node->cells) + i, (node->cells) + i + 1, sizeof(Cell));
    }
    node->numCells--;
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
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    return (int) left - 1;
}

int searchLeafNode(Page* node, uint64_t key, Cell* cell) {
    uint8_t size = node->numCells;
    uint8_t left = 0;
    uint8_t right = size;

    while (left < right) {
        uint8_t mid = left + (right - left) / 2;
        if (node->cells[mid].key <= key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    if (cell && left > 0) memcpy((void*) ((node->cells) + left - 1), (void*) cell, sizeof(Cell));
    return (int) left - 1;
}

int addRoot(int fd, Header* header, Page* root, Page* newPage, Page* child, off_t rootOffset) {
    Page* newRoot = mallocPage();
    initPage(newRoot, 1, INTERNAL_NODE, -1, -1, -1, rootOffset, root->offset);

    header->nodeNumber++;
    header->height++;
    header->rootOffset = rootOffset;

    root->isRoot = 1;
    root->parent = rootOffset;
    newPage->parent = rootOffset;

    if (dumpPage(fd, newRoot) < 0) {
        freePage(&newRoot);
        return -1;
    }

    off_t off;
    if ((off = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&newRoot);
        return -1;
    }
    newPage->offset = off;
    root->nextPage = newPage->offset;
    if (child) child->parent = off;
    if (dumpPage(fd, newPage) < 0 || dumpPage(fd, root) < 0 || dumpPage(fd, child) < 0) {
        freePage(&newRoot);
        return -1;
    }

    addCell(newRoot, -1, newPage->cells->key, newPage->offset, sizeof(Page));
    int ret = (int) dumpPage(fd, newRoot);
    freePage(&newRoot);
    return ret;
}

int addParentKey(int fd, Header* header, Page* prev, Page* newPage, uint64_t key) {
    if (dumpPage(fd, newPage) < 0 || dumpPage(fd, prev) < 0) return -1;
    return addInternalKey(fd, header, newPage, key);
}

Page* splitPage(int fd, Header* header, Page* page) {
    off_t recover;
    if ((recover = lseek(fd, 0, SEEK_END)) < 0) return NULL;

    Page* recoverPage = mallocPage();
    memcpy(recoverPage, page, sizeof(Page));

    Page* newPage = mallocPage();

    off_t off = recover;
    initPage(newPage, 0, page->type, page->parent, page->offset, page->nextPage, off, -1);

    uint8_t num = page->numCells;
    uint8_t half = num / 2;

    Cell* begin = page->cells;
    Cell* newBegin = newPage->cells;

    for (int i = half; i < num; i++) {
        memcpy(newBegin + i - half, begin + i, sizeof(Cell));
    }

    newPage->numCells = num - half;

    page->numCells = half;
    page->isRoot = 0;
    page->nextPage = newPage->offset;

    header->nodeNumber++;

    if (page->isRoot) {
        if (addRoot(fd, header, page, newPage, NULL, off) < 0) {
            ftruncate(fd, recover);
            while (dumpPage(fd, recoverPage) < 0);
            freePage(&newPage);
            freePage(&recoverPage);
            return NULL;
        }
    } else {
        if (addParentKey(fd, header, page, newPage, newPage->cells->key) < 0) {
            ftruncate(fd, recover);
            while (dumpPage(fd, recoverPage) < 0);
            freePage(&newPage);
            freePage(&recoverPage);
            return NULL;
        }
    }

    freePage(&newPage);
    if (dumpHeader(fd, header) < 0) {
        ftruncate(fd, recover);
        while (dumpPage(fd, recoverPage) < 0);
        freePage(&recoverPage);
        return NULL;
    }
    freePage(&recoverPage);

    return newPage;
}

int insertInternalPage(int fd, Header* header, Page* prev, Page* child, uint64_t key) {
    off_t recover;
    if ((recover = lseek(fd, 0, SEEK_END)) < 0) return -1;

    Page* recoverPrev = mallocPage();
    memcpy(recoverPrev, prev, sizeof(Page));
    Page* recoverChild = mallocPage();
    memcpy(recoverChild, child, sizeof(Page));

    Page* newPage = mallocPage();

    off_t off = recover;
    initPage(newPage, 0, INTERNAL_NODE, prev->parent, prev->offset, -1, off, -1);

    addCell(newPage, -1, key, child->offset, sizeof(Page));

    prev->nextPage = off;
    header->nodeNumber++;

    child->parent = off;
    if (dumpPage(fd, child) < 0) {
        freePage(&newPage);
        return -1;
    }

    if (prev->isRoot) {
        if (addRoot(fd, header, prev, newPage, child, off) < 0) {
            ftruncate(fd, recover);
            while (dumpPage(fd, recoverPrev) < 0);
            while (dumpPage(fd, recoverChild) < 0);
            freePage(&newPage);
            freePage(&recoverPrev);
            freePage(&recoverChild);
            return -1;
        }
    } else {
        if (addParentKey(fd, header, prev, newPage, key) < 0) {
            ftruncate(fd, recover);
            while (dumpPage(fd, recoverPrev) < 0);
            freePage(&newPage);
            freePage(&recoverPrev);
            freePage(&recoverChild);
            return -1;
        }
    }

    freePage(&newPage);
    if (dumpHeader(fd, header) < 0) {
        ftruncate(fd, recover);
        while (dumpPage(fd, recoverPrev) < 0);
        while (dumpPage(fd, recoverChild) < 0);
        freePage(&recoverPrev);
        freePage(&recoverChild);
        return -1;
    }
    freePage(&recoverPrev);
    freePage(&recoverChild);

    return 0;
}

int addInternalKey(int fd, Header* header, Page* child, uint64_t key) {
    Page* node = mallocPage();
    off_t nodeOffset = child->parent;
    if (loadPage(fd, nodeOffset, node) < 0) return -1;
    int pos = searchInternalNode(node, key);

    if (pos < 0) return -1;
    if (pos == MAX_CELL) return -1;

    if (node->cells[pos].key == key) return -1;

    if (pos == MAX_CELL - 1) {
        int ret = insertInternalPage(fd, header, node, child, key);
        freePage(&node);
        return ret;
    }

    if (node->numCells == MAX_CELL) {
        Page* newNode;
        if (!(newNode = splitPage(fd, header, node))) return -1;
        int pos1 = searchInternalNode(node, key);
        int pos2 = searchInternalNode(newNode, key);

        int ret;
        if (pos2 == -1) {
            addCell(node, pos1, key, child->offset, sizeof(Page));
            ret = (int) dumpPage(fd, node);
        } else {
            addCell(newNode, pos2, key, child->offset, sizeof(Page));
            ret = (int) dumpPage(fd, newNode);
        }
        freePage(&node);
        freePage(&newNode);
        return ret;
    }

    addCell(node, pos, key, child->offset, sizeof(Page));
    int ret = (int) dumpPage(fd, node);
    freePage(&node);

    return ret;
}

int insertLeafPage(int fd, Header* header, Page* prev, uint64_t key, off_t offset, size_t recordSize) {
    off_t recover;
    if ((recover = lseek(fd, 0, SEEK_END)) < 0) return -1;

    Page* recoverPrev = mallocPage();
    memcpy(recoverPrev, prev, sizeof(Page));

    Page* newLeaf = mallocPage();

    off_t off = recover;
    initPage(newLeaf, 0, LEAF_NODE, prev->parent, prev->offset, -1, off, -1);
    addCell(newLeaf, -1, key, offset, recordSize);

    prev->nextPage = newLeaf->offset;

    if (dumpPage(fd, newLeaf) < 0 || dumpPage(fd, prev) < 0) {
        ftruncate(fd, recover);
        return -1;
    }

    int ret = addInternalKey(fd, header, newLeaf, key);
    if (ret < 0) {
        ftruncate(fd, recover);
        while (dumpPage(fd, recoverPrev) < 0);
        return -1;
    }
    freePage(&newLeaf);
    return ret;
}

int openIndex(const char* filename, int oflag, Header* header) {
    int fd = open(filename, oflag);
    loadHeader(filename, header);
    return fd;
}

int createTree(int fd) {
    Header header;
    header.magicNumber = 0x1234;
    header.orderNumber = MAX_CELL;
    header.height = 2;
    header.nodeNumber = 3;

    header.rootOffset = sizeof(Header);

    write(fd, &header, sizeof(Header));

    Page* root = mallocPage();
    initPage(root, 1, INTERNAL_NODE, -1, -1, -1, -1, -1);
    if ((root->offset = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&root);
        return -1;
    }
    header.rootOffset = root->offset;
    dumpPage(fd, root);

    Page* leftLeaf = mallocPage();
    initPage(leftLeaf, 0, LEAF_NODE, root->offset, -1, -1, -1, -1);
    if ((leftLeaf->offset = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&root);
        freePage(&leftLeaf);
        return -1;
    }
    dumpPage(fd, leftLeaf);
    root->leftMost = leftLeaf->offset;

    freePage(&leftLeaf);

    Page* rightLeaf = mallocPage();
    initPage(rightLeaf, 0, LEAF_NODE, root->offset, leftLeaf->offset, -1, -1, -1);
    if ((rightLeaf->offset = lseek(fd, 0, SEEK_END)) < 0) {
        freePage(&root);
        freePage(&rightLeaf);
        return -1;
    }
    dumpPage(fd, rightLeaf);
    addCell(root, -1, MAX_CELL, rightLeaf->offset, sizeof(Page));

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

int search(int fd, Header* header, Page* node, uint64_t key, Cell* cell) {
    int flag = 0;
    if (!node) {
        node = mallocPage();
        flag = 1;
    }
    if (loadPage(fd, header->rootOffset, node) < 0) return -1;
    while (node->type == INTERNAL_NODE) {
        int pos = searchInternalNode(node, key);
        if (pos < 0) return -1;

        off_t offset;
        if (pos >= MAX_CELL) offset = node->leftMost;
        else offset = node->cells[pos].offset;

        if (loadPage(fd, offset, node) < 0) return -1;
    }

    int ret = searchLeafNode(node, key, cell);
    if (flag) freePage(&node);
    return ret;
}

int insert(int fd, Header* header, uint64_t key, off_t offset, size_t recordSize) {
    Page* node = mallocPage();

    int pos = search(fd, header, node, key, NULL);
    if (pos < 0) {
        freePage(&node);
        return -1;
    }

    Page* leaf = node;
    node = NULL;
    if (leaf->cells[pos].key == key) {
        freePage(&leaf);
        return -1;
    }

    if (pos == MAX_CELL - 1) {
        int ret = insertLeafPage(fd, header, leaf, key, offset, recordSize);
        freePage(&leaf);
        return ret;
    }

    if (leaf->numCells == MAX_CELL) {
        Page* newLeaf;
        if (!(newLeaf = splitPage(fd, header, leaf))) return -1;
        int pos1 = searchLeafNode(leaf, key, NULL);
        int pos2 = searchLeafNode(newLeaf, key, NULL);

        int ret;
        if (pos2 == -1) {
            addCell(leaf, pos1, key, offset, recordSize);
            ret = (int) dumpPage(fd, leaf);
        } else {
            addCell(newLeaf, pos2, key, offset, recordSize);
            ret = (int) dumpPage(fd, newLeaf);
        }
        freePage(&newLeaf);
        freePage(&leaf);
        return ret;
    }

    addCell(leaf, pos, key, offset, recordSize);
    int ret = (int) dumpPage(fd, leaf);
    freePage(&leaf);
    return ret;
}

int delete(int fd, Header* header, uint64_t key, Cell* cell) {
    Page* node = mallocPage();

    int pos = search(fd, header, node, key, NULL);
    if (pos < 0) {
        freePage(&node);
        return -1;
    }

    Page* leaf = node;
    node = NULL;
    if (leaf->cells[pos].key != key) {
        freePage(&leaf);
        return -1;
    }

    int ret = deleteCell(fd, leaf, pos, cell);
    if (dumpPage(fd, leaf) < 0) return -1;
    freePage(&leaf);
    return ret;
}

int update(int fd, Header* header, uint64_t key, Cell* cell) {
    Page* node = mallocPage();
    int pos = search(fd, header, node, key, NULL);
    if (pos < 0) {
        freePage(&node);
        return -1;
    }

    Page* leaf = node;
    node = NULL;
    if (leaf->cells[pos].key != key) {
        freePage(&leaf);
        return -1;
    }
    memcpy((leaf->cells) + pos, cell, sizeof(Cell));
    freePage(&leaf);
    return 0;
}

#pragma clang diagnostic pop