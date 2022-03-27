//
// Created by Machearn Ning on 2/4/22.
//

#ifndef MDBM_BTREE_H
#define MDBM_BTREE_H

#include <sys/types.h>

#define MAX_CELL 168

typedef struct Cell Cell;
typedef struct Page Page;
typedef struct Header Header;

typedef enum {
    LEAF_NODE,
    INTERNAL_NODE,
}NodeType;

struct Header{
    int magicNumber;
    size_t orderNumber;
    size_t nodeNumber;
    size_t height;

    off_t rootOffset;
    off_t mostLeftLeafOffset;
};

struct Cell {
    uint64_t key;
    size_t size; // if page is leaf, it is the size of record, else it is the size of subpage.
    off_t offset; // if page is leaf, it is the offset of record, else it is the offset of subpage.
};

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
void freePage(Page** page);
Cell* mallocCell();
void freeCell(Cell** cell);

int openIndex(const char* filename, int oflag, Header* header);

int createTree(int fd);

ssize_t insert(int fd, Header* header, Page* leaf, int pos, const Cell* cell);
int search(int fd, Header* header, Page* node, uint64_t key, Cell* Cell);
ssize_t delete(int fd, Page* leaf, int pos);
ssize_t update(int fd, Page* leaf, int pos, const Cell* cell);

#endif //MDBM_BTREE_H
