//
// Created by Machearn Ning on 3/31/22.
//

#ifndef MDBM_DATA_H
#define MDBM_DATA_H

#include <sys/types.h>

#define PAYLOAD_SIZE 4072

typedef struct DataPage DataPage;

struct DataPage {
    off_t offset;
    size_t num_tuples;
    int16_t payload_tail;
    char data[PAYLOAD_SIZE];
};

DataPage* malloc_data_page();
void free_data_page(DataPage** page);

ssize_t insert_data(int fd, DataPage* page, off_t page_offset, char* tuple, size_t tuple_size);
ssize_t fetch_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, void* tuple, size_t tuple_size);
ssize_t update_tuple(int fd, DataPage* page, off_t page_offset, size_t slot_index, size_t old_tuple_size, char* new_tuple, size_t tuple_size);
ssize_t delete_tuple(int fd, DataPage* page, off_t page_offset, size_t slot, size_t tuple_size);

#endif //MDBM_DATA_H
