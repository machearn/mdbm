//
// Created by Machearn Ning on 2/4/22.
//

#ifndef MDBM_BTREE_H
#define MDBM_BTREE_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define MAX_CELL 126

typedef struct Cell Cell;
typedef struct Page Page;

/*
 * total size of Cell is 4*8 bytes
 */
struct Cell {
    uint64_t key;
    off_t offset; // if page is leaf, it is the offset of record, else it is the offset of subpage.
    Cell* nextCell;
    Cell* prevCell;
};

typedef enum {
    LEAF_NODE,
    INTERNAL_NODE,
}NodeType;

/*
 * page size is 4KB aligned
 */
struct Page {
    NodeType type; // Leaf or Internal
    uint8_t isRoot;
    uint8_t numCells;
    off_t offset;
    off_t leftMost; // only for internal node left most subpage which contains keys smaller than all keys in the node.
    off_t parent;
    off_t prevPage;
    off_t nextPage;
    Cell cells[MAX_CELL];
    char padding[16];
};

Page* mallocPage();
int freePage(Page** page);

ssize_t dumpPage(int fd, Page* page);
ssize_t loadPage(int fd, off_t offset, Page* page);
int initPage(Page* page, uint8_t isRoot, uint8_t type, off_t parent, off_t prev, uint64_t key, off_t offset);

int searchInternalNode(Page* node, uint64_t key);
int searchLeafNode(Page* node, uint64_t key, Cell* Cell);

Page* splitNode(int fd, Page* node);
int insertInternalPage(int fd, Page* prev, uint64_t key, off_t offset);
int addInternalKey(int fd, off_t nodeOff, uint64_t key, off_t offset);
int addCell(Page* leaf, int pos, uint64_t key, off_t offset);
int insertLeafPage(int fd, Page* prev, uint64_t key, off_t offset);

int insert(int fd, Page* root, uint64_t key, off_t offset);
int search(int fd, Page* root, uint64_t key, Cell* Cell);
int delete(int fd, Page* root, uint64_t key);
int update(int fd, Page* root, uint64_t key, Cell* cell);


#endif //MDBM_BTREE_H
