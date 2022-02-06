//
// Created by Machearn Ning on 2/4/22.
//

#ifndef MDBM_BTREE_H
#define MDBM_BTREE_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define MAX_CELL 127

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
    off_t leftMost; // only for internal node left most subpage which contains keys smaller than all keys in the node.
    Cell cells[MAX_CELL];
    off_t parent;
    NodeType type; // Leaf or Internal
    int fd; // if Root, fd is the index file else -1.
    uint8_t numCells;
    uint8_t wasted[4];
};

Page* mallocPage();

int loadPage(int fd, off_t offset, Page** pPage);

off_t searchInternalNode(Page* node, uint64_t key);
int searchLeafNode(Page* node, uint64_t key, Cell* Cell);

int insert(int fd, Page* root, uint64_t key, off_t offset);
int search(int fd, Page* root, Cell** pCell);
int delete(int fd, uint64_t key);
int update(int fd, Page* root, uint64_t key, Cell* cell);

int splitNode(int fd, Page* Node);

#endif //MDBM_BTREE_H
