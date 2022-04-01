//
// Created by Machearn Ning on 3/31/22.
//

#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "data.h"
#include "lock.h"

DataPage* malloc_data_page() {
    DataPage* data_page = (DataPage*)malloc(sizeof(DataPage));
    memset(data_page, 0, sizeof(DataPage));
    return data_page;
}

void free_data_page(DataPage** data_page) {
    free(*data_page);
    *data_page = NULL;
}

ssize_t dump_data_page(int fd, DataPage* data_page) {
    if (write_lock_wait(fd, data_page->offset, SEEK_SET, sizeof(DataPage)) == -1) return -1;
    if (lseek(fd, data_page->offset, SEEK_SET) < 0) return -1;
    ssize_t ret = write(fd, data_page, sizeof(DataPage));
    if (unlock(fd, data_page->offset, SEEK_SET, sizeof(DataPage)) == -1) return -1;
    return ret;
}

ssize_t load_data_page(int fd, DataPage* data_page, off_t offset) {
    if (read_lock_wait(fd, offset, SEEK_SET, sizeof(DataPage)) == -1) return -1;
    if (lseek(fd, offset, SEEK_SET) < 0) return -1;
    ssize_t ret = read(fd, data_page, sizeof(DataPage));
    if (unlock(fd, offset, SEEK_SET, sizeof(DataPage)) == -1) return -1;
    return ret;
}

size_t get_tuple_size(DataPage* page, size_t slot_index) {
    if (slot_index >= page->num_tuples) return 0;

    uint16_t* slot = (uint16_t*)page->data;
    size_t tail = 4096;
    if (slot_index > 0)
        tail = slot[slot_index - 1];

    return tail - slot[slot_index];
}

ssize_t insert_data(int fd, DataPage* page, off_t page_offset, char* tuple, size_t tuple_size) {
    load_data_page(fd, page, page_offset);

    uint16_t* slot = (uint16_t*)page->data;
    uint16_t tail = 4096;
    int i = 0;
    for (i = 0; i < page->num_tuples; i++) {
        if (slot[i] > 0) tail = slot[i];
        else break;
    }

    uint16_t tuple_offset = tail - tuple_size;
    if (page->data[tuple_offset] != 0) return -1;
    memcpy(page->data + tuple_offset, tuple, tuple_size);
    slot[i] = tuple_offset;
    page->num_tuples++;

    return dump_data_page(fd, page);
}

ssize_t fetch_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, void* tuple) {
    load_data_page(fd, page, page_offset);
    if (slot_index >= page->num_tuples) return -1;

    uint16_t* slot = (uint16_t*)page->data;
    size_t tuple_size = get_tuple_size(page, slot_index);
    uint16_t tuple_offset = slot[slot_index];

    memcpy(tuple, page->data + tuple_offset, tuple_size);
    return 0;
}

ssize_t update_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, char* new_tuple, size_t tuple_size) {
    load_data_page(fd, page, page_offset);
    if (slot_index >= page->num_tuples) return -1;

    uint16_t* slot = (uint16_t*)page->data;
    size_t old_tuple_size = get_tuple_size(page, slot_index);
    uint16_t offset = slot[slot_index];

    if (tuple_size <= old_tuple_size) {
        memcpy(page->data + offset, new_tuple, tuple_size);
    } else {
        memset(page->data + offset, 0, old_tuple_size);
        uint16_t tail = slot[page->num_tuples - 1];
        uint16_t new_offset = tail - tuple_size;
        if (page->data[new_offset] != 0) return -1;
        memcpy(page->data + new_offset, new_tuple, tuple_size);
        slot[slot_index] = new_offset;
    }
    return dump_data_page(fd, page);
}

ssize_t delete_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index) {
    load_data_page(fd, page, page_offset);
    if (slot_index >= page->num_tuples) return -1;

    uint16_t* slot = (uint16_t*)page->data;
    size_t tuple_size = get_tuple_size(page, slot_index);
    uint16_t offset = slot[slot_index];

    memset(page->data + offset, 0, tuple_size);
    for (size_t i = slot_index; i < page->num_tuples - 1; i++) {
        slot[i] = slot[i + 1];
    }
    slot[page->num_tuples - 1] = 0;
    page->num_tuples--;

    return dump_data_page(fd, page);
}
