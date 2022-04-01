//
// Created by Machearn Ning on 2/4/22.
//

#ifndef MDBM_BTREE_H
#define MDBM_BTREE_H

#include <sys/types.h>

#define MAX_CELL 168

typedef struct Cell Cell;
typedef struct IndexPage IndexPage;
typedef struct Header Header;

typedef enum {
    LEAF_NODE,
    INTERNAL_NODE,
}NodeType;

struct Header {
    int magic_number;
    size_t node_number;
    size_t height;

    off_t root_offset;
    off_t left_most_leaf_offset;
};

struct Cell {
    uint64_t key;
    off_t offset; // if page is leaf, it is the offset of data page, else it is the offset of subpage.
    size_t slot; // the index of slot.
};

struct IndexPage {
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

IndexPage* malloc_index_page();
void free_index_page(IndexPage** page);
Cell* malloc_cell();
void free_cell(Cell** cell);

int first_key(int fd, Header* header, IndexPage* leaf, Cell* cell);
int next_key(int fd, IndexPage* leaf, int* pos, Cell* cell);

int load_index_header(int fd, Header* header);

int create_tree(int fd);
int get_left_most_leaf(int fd, Header* header, IndexPage* leaf);

ssize_t insert_index(int fd, Header* header, IndexPage* leaf, int pos, const Cell* cell);
int search_index(int fd, Header* header, IndexPage* node, uint64_t key, Cell* Cell);
ssize_t delete_index(int fd, IndexPage* leaf, int pos);
ssize_t update_index(int fd, IndexPage* leaf, int pos, const Cell* cell);

#endif //MDBM_BTREE_H
