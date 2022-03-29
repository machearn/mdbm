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

struct Header {
    int magic_number;
    size_t order_number;
    size_t node_number;
    size_t height;

    off_t root_offset;
    off_t left_most_leaf_offset;
};

struct Cell {
    uint64_t key;
    size_t size; // if page is leaf, it is the size of record, else it is the size of subpage.
    off_t offset; // if page is leaf, it is the offset of record, else it is the offset of subpage.
};

struct Page {
    NodeType type; // Leaf or Internal
    uint8_t is_root;
    uint8_t num_cells;
    off_t offset;
    off_t left_most; // only for internal node left most subpage which contains keys smaller than all keys in the node.
    off_t parent;
    off_t prev_page;
    off_t next_page;
    Cell cells[MAX_CELL];
    char padding[16];
};

Page* malloc_page();
void free_page(Page** page);
Cell* malloc_cell();
void free_cell(Cell** cell);

int first_key(int fd, Header* header, Page* leaf, Cell* cell);
int next_key(int fd, Page* leaf, uint64_t key, Cell* cell);

int open_index(const char* file_name, int oflag, Header* header);

int create_tree(int fd);

ssize_t insert(int fd, Header* header, Page* leaf, int pos, const Cell* cell);
int search(int fd, Header* header, Page* node, uint64_t key, Cell* Cell);
ssize_t delete(int fd, Page* leaf, int pos);
ssize_t update(int fd, Page* leaf, int pos, const Cell* cell);

#endif //MDBM_BTREE_H
