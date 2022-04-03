//
// Created by Machearn Ning on 2/4/22.
//

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "lock.h"
#include "btree.h"

IndexPage* split_page(int fd, Header* header, IndexPage* page);

int add_internal_key(int fd, Header* header, IndexPage* child, uint64_t key);

IndexPage* malloc_index_page() {
    IndexPage* p = NULL;
    p = (IndexPage*) malloc(sizeof(IndexPage));
    memset(p, 0, sizeof(IndexPage));
    return p;
}

void free_index_page(IndexPage** page) {
    free(*page);
    *page = NULL;
}

Cell* malloc_cell() {
    Cell* c = NULL;
    c = (Cell*) malloc(sizeof(Cell));
    memset(c, 0, sizeof(Cell));
    return c;
}

void free_cell(Cell** cell) {
    free(*cell);
    *cell = NULL;
}

ssize_t load_page(int fd, off_t offset, IndexPage* page) {
    off_t off;
    if ((off = lseek(fd, offset, SEEK_SET)) < 0) return -1;
    if (read_lock_wait(fd, off, SEEK_SET, sizeof(IndexPage)) < 0) return -1;
    ssize_t ret = read(fd, page, sizeof(IndexPage));
    if (unlock(fd, off, SEEK_SET, sizeof(IndexPage)) < 0) return -1;
    return ret;
}

ssize_t dump_page(int fd, IndexPage* page) {
    if (!page) return 0;

    off_t offset = page->offset;
    if (lseek(fd, offset, SEEK_SET) < 0) return -1;
    if (write_lock_wait(fd, offset, SEEK_SET, sizeof(IndexPage)) < 0) return -1;
    ssize_t ret = write(fd, page, sizeof(IndexPage));
    if (unlock(fd, offset, SEEK_SET, sizeof(IndexPage)) < 0) return -1;
    return ret;
}

ssize_t load_header(int fd, Header* header) {
    if (read_lock_wait(fd, 0, SEEK_SET, sizeof(Header)) < 0) return -1;
    ssize_t ret = read(fd, header, sizeof(Header));
    if (unlock(fd, 0, SEEK_SET, sizeof(Header)) < 0) return -1;
    return ret;
}

ssize_t dump_header(int fd, Header* header) {
    if (lseek(fd, 0, SEEK_SET) < 0) return -1;
    if (write_lock_wait(fd, 0, SEEK_SET, sizeof(Header)) < 0) return -1;
    ssize_t ret = write(fd, header, sizeof(Header));
    if (unlock(fd, 0, SEEK_SET, sizeof(Header)) < 0) return -1;
    return ret;
}

int init_page(IndexPage* page, uint8_t is_root, uint8_t type, off_t parent, off_t prev, off_t next,
              off_t offset, off_t left_most) {
    page->num_cells = 0;
    page->is_root = is_root;
    page->parent = parent;
    page->next_page = next;
    page->prev_page = prev;
    page->type = type;
    page->left_most = left_most;
    page->offset = offset;
    return 0;
}

int add_cell(IndexPage* leaf, int pos, uint64_t key, off_t offset, uint16_t slot) {
    Cell* begin = leaf->cells;
    for (int i = leaf->num_cells - 1; i > pos; i--) {
        memcpy(begin + i + 1, begin + i, sizeof(Cell));
    }

    begin[pos + 1].key = key;
    begin[pos + 1].offset = offset;
    begin[pos + 1].slot_index = slot;

    leaf->num_cells++;

    return 0;
}

int delete_cell(IndexPage* node, int pos) {
    if (node->num_cells < 1) return -1;

    for (int i = pos; i < node->num_cells - 1; i++) {
        memcpy((node->cells) + i, (node->cells) + i + 1, sizeof(Cell));
    }
    node->num_cells--;
    return 0;
}

int search_internal_node(IndexPage* node, uint64_t key) {
    uint8_t size = node->num_cells;
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

int search_leaf_node(IndexPage* node, uint64_t key, Cell* cell) {
    uint8_t size = node->num_cells;
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

    if (cell && left > 0) memcpy((void*) cell, (void*) ((node->cells) + left - 1), sizeof(Cell));
    return (int) left - 1;
}

int add_root(int fd, Header* header, IndexPage* root, IndexPage* new_page, IndexPage* child, off_t root_offset) {
    IndexPage* new_root = malloc_index_page();
    init_page(new_root, 1, INTERNAL_NODE, -1, -1, -1, root_offset, root->offset);

    header->node_number++;
    header->height++;
    header->root_offset = root_offset;

    root->is_root = 1;
    root->parent = root_offset;
    new_page->parent = root_offset;

    if (dump_page(fd, new_root) < 0) {
        free_index_page(&new_root);
        return -1;
    }

    off_t off;
    if ((off = lseek(fd, 0, SEEK_END)) < 0) {
        free_index_page(&new_root);
        return -1;
    }
    new_page->offset = off;
    root->next_page = new_page->offset;
    if (child) child->parent = off;
    if (dump_page(fd, new_page) < 0 || dump_page(fd, root) < 0 || dump_page(fd, child) < 0) {
        free_index_page(&new_root);
        return -1;
    }

    add_cell(new_root, -1, new_page->cells->key, new_page->offset, sizeof(IndexPage));
    int ret = (int) dump_page(fd, new_root);
    free_index_page(&new_root);
    return ret;
}

int add_parent_key(int fd, Header* header, IndexPage* prev, IndexPage* new_page, uint64_t key) {
    if (dump_page(fd, new_page) < 0 || dump_page(fd, prev) < 0) return -1;
    return add_internal_key(fd, header, new_page, key);
}

IndexPage* split_page(int fd, Header* header, IndexPage* page) {
    off_t recover;
    if ((recover = lseek(fd, 0, SEEK_END)) < 0) return NULL;

    IndexPage* recover_page = malloc_index_page();
    memcpy(recover_page, page, sizeof(IndexPage));

    IndexPage* new_page = malloc_index_page();

    off_t off = recover;
    init_page(new_page, 0, page->type, page->parent, page->offset, page->next_page, off, -1);

    uint8_t num = page->num_cells;
    uint8_t half = num / 2;

    Cell* begin = page->cells;
    Cell* new_begin = new_page->cells;

    for (int i = half; i < num; i++) {
        memcpy(new_begin + i - half, begin + i, sizeof(Cell));
    }

    new_page->num_cells = num - half;

    page->num_cells = half;
    page->is_root = 0;
    page->next_page = new_page->offset;

    header->node_number++;

    if (page->is_root) {
        if (add_root(fd, header, page, new_page, NULL, off) < 0) {
            ftruncate(fd, recover);
            while (dump_page(fd, recover_page) < 0);
            free_index_page(&new_page);
            free_index_page(&recover_page);
            return NULL;
        }
    } else {
        if (add_parent_key(fd, header, page, new_page, new_page->cells->key) < 0) {
            ftruncate(fd, recover);
            while (dump_page(fd, recover_page) < 0);
            free_index_page(&new_page);
            free_index_page(&recover_page);
            return NULL;
        }
    }

    free_index_page(&new_page);
    if (dump_header(fd, header) < 0) {
        ftruncate(fd, recover);
        while (dump_page(fd, recover_page) < 0);
        free_index_page(&recover_page);
        return NULL;
    }
    free_index_page(&recover_page);

    return new_page;
}

int insert_internal_page(int fd, Header* header, IndexPage* prev, IndexPage* child, uint64_t key) {
    off_t recover;
    if ((recover = lseek(fd, 0, SEEK_END)) < 0) return -1;

    IndexPage* recover_prev = malloc_index_page();
    memcpy(recover_prev, prev, sizeof(IndexPage));
    IndexPage* recover_child = malloc_index_page();
    memcpy(recover_child, child, sizeof(IndexPage));

    IndexPage* new_page = malloc_index_page();

    off_t off = recover;
    init_page(new_page, 0, INTERNAL_NODE, prev->parent, prev->offset, -1, off, -1);

    add_cell(new_page, -1, key, child->offset, sizeof(IndexPage));

    prev->next_page = off;
    header->node_number++;

    child->parent = off;
    if (dump_page(fd, child) < 0) {
        free_index_page(&new_page);
        return -1;
    }

    if (prev->is_root) {
        if (add_root(fd, header, prev, new_page, child, off) < 0) {
            ftruncate(fd, recover);
            while (dump_page(fd, recover_prev) < 0);
            while (dump_page(fd, recover_child) < 0);
            free_index_page(&new_page);
            free_index_page(&recover_prev);
            free_index_page(&recover_child);
            return -1;
        }
    } else {
        if (add_parent_key(fd, header, prev, new_page, key) < 0) {
            ftruncate(fd, recover);
            while (dump_page(fd, recover_prev) < 0);
            free_index_page(&new_page);
            free_index_page(&recover_prev);
            free_index_page(&recover_child);
            return -1;
        }
    }

    free_index_page(&new_page);
    if (dump_header(fd, header) < 0) {
        ftruncate(fd, recover);
        while (dump_page(fd, recover_prev) < 0);
        while (dump_page(fd, recover_child) < 0);
        free_index_page(&recover_prev);
        free_index_page(&recover_child);
        return -1;
    }
    free_index_page(&recover_prev);
    free_index_page(&recover_child);

    return 0;
}

int init_root(int fd, Header* header, IndexPage* left_child, uint64_t key) {
    IndexPage* root = malloc_index_page();
    off_t root_offset;
    if ((root_offset = lseek(fd, 0, SEEK_END)) < 0) {
        free_index_page(&root);
        return -1;
    }
    init_page(root, 1, INTERNAL_NODE, -1, -1, -1, root_offset, left_child->offset);

    root->cells[0].key = key;
    root->cells[0].offset = left_child->next_page;
    root->cells[0].slot_index = -1;
    root->num_cells = 1;

    header->node_number++;
    header->height++;
    header->root_offset = root_offset;

    if (dump_page(fd, root) < 0) {
        free_index_page(&root);
        return -1;
    }
    if (dump_header(fd, header) < 0) {
        free_index_page(&root);
        return -1;
    }
    return 0;
}

int add_internal_key(int fd, Header* header, IndexPage* child, uint64_t key) {
    IndexPage* node = malloc_index_page();
    off_t node_offset = child->parent;

    if (node_offset == -1) return init_root(fd, header, child, key);

    if (load_page(fd, node_offset, node) < 0) return -1;
    int pos = search_internal_node(node, key);

    if (pos < 0) return -1;
    if (pos == MAX_CELL) return -1;

    if (node->cells[pos].key == key) return -1;

    if (pos == MAX_CELL - 1) {
        int ret = insert_internal_page(fd, header, node, child, key);
        free_index_page(&node);
        return ret;
    }

    if (node->num_cells == MAX_CELL) {
        IndexPage* new_node;
        if (!(new_node = split_page(fd, header, node))) return -1;
        int pos1 = search_internal_node(node, key);
        int pos2 = search_internal_node(new_node, key);

        int ret;
        if (pos2 == -1) {
            add_cell(node, pos1, key, child->offset, sizeof(IndexPage));
            ret = (int) dump_page(fd, node);
        } else {
            add_cell(new_node, pos2, key, child->offset, sizeof(IndexPage));
            ret = (int) dump_page(fd, new_node);
        }
        free_index_page(&node);
        free_index_page(&new_node);
        return ret;
    }

    add_cell(node, pos, key, child->offset, sizeof(IndexPage));
    int ret = (int) dump_page(fd, node);
    free_index_page(&node);

    return ret;
}

int insert_leaf_page(int fd, Header* header, IndexPage* prev, const Cell* cell) {
    off_t recover;
    if ((recover = lseek(fd, 0, SEEK_END)) < 0) return -1;

    IndexPage* recover_prev = malloc_index_page();
    memcpy(recover_prev, prev, sizeof(IndexPage));

    IndexPage* new_leaf = malloc_index_page();

    off_t off = recover;
    init_page(new_leaf, 0, LEAF_NODE, prev->parent, prev->offset, -1, off, -1);
    add_cell(new_leaf, -1, cell->key, cell->offset, cell->slot_index);

    prev->next_page = new_leaf->offset;

    if (dump_page(fd, new_leaf) < 0 || dump_page(fd, prev) < 0) {
        ftruncate(fd, recover);
        return -1;
    }

    int ret = add_internal_key(fd, header, new_leaf, cell->key);
    free_index_page(&new_leaf);
    if (ret < 0) {
        ftruncate(fd, recover);
        while (dump_page(fd, recover_prev) < 0);
        return -1;
    }
    return ret;
}

int load_index_header(int fd, Header* header) {
    load_header(fd, header);
    return fd;
}

int create_tree(int fd) {
    Header header;
    header.magic_number = 0x1234;
    header.height = 1;
    header.node_number = 1;

    header.root_offset = -1;

    if (dump_header(fd, &header) < 0) return -1;

    IndexPage* left_leaf = malloc_index_page();
    init_page(left_leaf, 0, LEAF_NODE, -1, -1, -1, -1, -1);
    if ((left_leaf->offset = lseek(fd, 0, SEEK_END)) < 0) {
        free_index_page(&left_leaf);
        return -1;
    }
    if (dump_page(fd, left_leaf) < 0) {
        free_index_page(&left_leaf);
        return -1;
    }

    header.left_most_leaf_offset = left_leaf->offset;
    free_index_page(&left_leaf);
    if (dump_header(fd, &header) < 0) return -1;
    return 0;
}

int get_left_most_leaf(int fd, Header* header, IndexPage* leaf) {
    if (load_page(fd, header->left_most_leaf_offset, leaf) < 0) return -1;
    return 0;
}

int search_index(int fd, Header* header, IndexPage* node, uint64_t key, Cell* Cell) {
    int flag = 0;
    if (!node) {
        node = malloc_index_page();
        flag = 1;
    }
    off_t off = header->root_offset < 0 ? header->left_most_leaf_offset : header->root_offset;

    if (load_page(fd, off, node) < 0) return -1;
    while (node->type == INTERNAL_NODE) {
        int pos = search_internal_node(node, key);
        if (pos < 0) return -1;

        off_t offset;
        if (pos >= MAX_CELL) offset = node->left_most;
        else offset = node->cells[pos].offset;

        if (load_page(fd, offset, node) < 0) return -1;
    }

    int ret = search_leaf_node(node, key, Cell);
    if (flag) free_index_page(&node);
    return ret;
}

ssize_t insert_index(int fd, Header* header, IndexPage* leaf, int pos, const Cell* cell) {
    if (pos == MAX_CELL - 1) {
        int ret = insert_leaf_page(fd, header, leaf, cell);
        return ret;
    }

    if (leaf->num_cells == MAX_CELL) {
        IndexPage* new_leaf;
        if (!(new_leaf = split_page(fd, header, leaf))) return -1;
        int pos1 = search_leaf_node(leaf, cell->key, NULL);
        int pos2 = search_leaf_node(new_leaf, cell->key, NULL);

        int ret;
        if (pos2 == -1) {
            add_cell(leaf, pos1, cell->key, cell->offset, cell->slot_index);
            ret = (int) dump_page(fd, leaf);
        } else {
            add_cell(new_leaf, pos2, cell->key, cell->offset, cell->slot_index);
            ret = (int) dump_page(fd, new_leaf);
        }
        free_index_page(&new_leaf);
        return ret;
    }

    add_cell(leaf, pos, cell->key, cell->offset, cell->slot_index);
    return dump_page(fd, leaf);
}

ssize_t delete_index(int fd, IndexPage* leaf, int pos) {
    int ret = delete_cell(leaf, pos);
    if (dump_page(fd, leaf) < 0) return -1;
    return ret;
}

ssize_t update_index(int fd, IndexPage* leaf, int pos, const Cell* cell) {
    memcpy((leaf->cells) + pos, cell, sizeof(Cell));
    return dump_page(fd, leaf);
}

int first_key(int fd, Header* header, IndexPage* leaf, Cell* cell) {
    if (load_page(fd, header->left_most_leaf_offset, leaf) < 0) return -1;
    while (leaf->num_cells == 0) {
        off_t next = leaf->next_page;
        if (load_page(fd, next, leaf) < 0) return -1;
    }
    memcpy(cell, leaf->cells, sizeof(Cell));
    return 0;
}

int next_key(int fd, IndexPage* leaf, int* pos, Cell* cell) {
    if (*pos == leaf->num_cells - 1) {
        do {
            off_t next = leaf->next_page;
            if (next == -1) return -2;
            if (load_page(fd, next, leaf) < 0) return -1;
        } while (leaf->num_cells == 0);

        *pos = 0;
        memcpy(cell, leaf->cells, sizeof(Cell));
    } else {
        (*pos)++;
        memcpy(cell, leaf->cells + (*pos), sizeof(Cell));
    }
    return 0;
}
