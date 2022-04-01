//
// Created by Machearn Ning on 3/31/22.
//

#ifndef MDBM_DATA_H
#define MDBM_DATA_H

#include <sys/types.h>

typedef struct DataPage DataPage;

struct DataPage {
    off_t offset;
    size_t num_tuples;
    char data[4080];
};

DataPage* malloc_data_page();
void free_data_page(DataPage** page);

size_t get_tuple_size(DataPage* page, size_t slot_index);

ssize_t insert_data(int fd, DataPage* page, off_t page_offset, char* tuple, size_t tuple_size);
ssize_t fetch_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, void* tuple);
ssize_t update_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, char* new_tuple, size_t tuple_size);
ssize_t delete_tuple(int fd, DataPage* page, off_t page_offset, size_t slot);

#endif //MDBM_DATA_H
