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

ssize_t init_data_page(DataPage* data_page) {
    data_page->num_tuples = 0;
    data_page->offset = -1;
    memset(data_page->data, 0, PAYLOAD_SIZE);
    return 0;
}

ssize_t add_data_page(int fd, void* tuple, size_t tuple_size) {
    DataPage* data_page = malloc_data_page();
    init_data_page(data_page);
    int16_t* slot = (int16_t*)data_page->data;
    int16_t tail = 4096;
    int16_t offset = (int16_t)(tail - tuple_size);

    memcpy(data_page->data + offset, tuple, tuple_size);
    slot[0] = offset;
    data_page->num_tuples++;

    if ((data_page->offset = lseek(fd, 0, SEEK_END)) < 0) {
        free_data_page(&data_page);
        return -1;
    }

    ssize_t ret = dump_data_page(fd, data_page);
    off_t page_offset = data_page->offset;
    free_data_page(&data_page);
    return ret < 0 ? -1 : page_offset;
}

ssize_t insert_data(int fd, DataPage* page, off_t page_offset, char* tuple, size_t tuple_size) {
    load_data_page(fd, page, page_offset);

    int16_t* slot = (int16_t*)page->data;
    int16_t tail = 4096;
    int i = 0;
    for (i = 0; i < page->num_tuples; i++) {
        if (slot[i] > 0) tail = slot[i];
        else break;
    }

    int16_t tuple_offset = (int16_t)(tail - tuple_size);
    if (page->data[tuple_offset] != 0) {
        return add_data_page(fd, tuple, tuple_size);
    }
    memcpy(page->data + tuple_offset, tuple, tuple_size);
    slot[i] = tuple_offset;
    page->num_tuples++;

    ssize_t ret = dump_data_page(fd, page);
    return ret < 0 ? -1 : page_offset;
}

ssize_t fetch_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, void* tuple, size_t tuple_size) {
    load_data_page(fd, page, page_offset);
    if (slot_index >= page->num_tuples) return -1;

    int16_t* slot = (int16_t*)page->data;
    int16_t tuple_offset = slot[slot_index];

    memcpy(tuple, page->data + tuple_offset, tuple_size);
    return 0;
}

ssize_t update_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, size_t old_tuple_size, char* new_tuple, size_t tuple_size) {
    load_data_page(fd, page, page_offset);
    if (slot_index >= page->num_tuples) return -1;

    int16_t* slot = (int16_t*)page->data;
    int16_t offset = slot[slot_index];

    if (tuple_size == old_tuple_size) {
        memcpy(page->data + offset, new_tuple, tuple_size);
    } else {
        memset(page->data + offset, 0, old_tuple_size);
        int16_t new_offset = (int16_t)(page->payload_tail - tuple_size);

        if (page->data[new_offset] != 0) {
            return add_data_page(fd, new_tuple, tuple_size);
        }
        memcpy(page->data + new_offset, new_tuple, tuple_size);
        slot[slot_index] = new_offset;
    }
    ssize_t ret = dump_data_page(fd, page);
    return ret < 0 ? -1 : page_offset;
}

ssize_t delete_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, size_t tuple_size) {
    load_data_page(fd, page, page_offset);
    if (slot_index >= page->num_tuples) return -1;

    int16_t* slot = (int16_t*)page->data;
    int16_t offset = slot[slot_index];

    memset(page->data + offset, 0, tuple_size);
    if (offset == page->payload_tail) {
        page->payload_tail = (int16_t) (offset - tuple_size);
    }
    slot[slot_index] = -1;
    page->num_tuples--;

    return dump_data_page(fd, page);
}
