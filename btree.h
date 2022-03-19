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

#define MAX_CELL 252

typedef struct Cell Cell;
typedef struct Page Page;

typedef struct {
    int magicNumber;
    size_t orderNumber;
    size_t nodeNumber;
    size_t height;

    off_t rootOffset;
    off_t mostLeftLeafOffset;
}Header;

/*
 * total size of Cell is 4*8 bytes
 */
struct Cell {
    uint64_t key;
    off_t offset; // if page is leaf, it is the offset of record, else it is the offset of subpage.
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

int openIndex(const char* filename, Header* header);

int createTree(const char* fileName);

int insert(int fd, Header* header, uint64_t key, off_t offset);
int search(int fd, Header* header, Page* node, uint64_t key, Cell* Cell);
int delete(int fd, Header* header, uint64_t key);
int update(int fd, Header* header, uint64_t key, Cell* cell);


#endif //MDBM_BTREE_H
