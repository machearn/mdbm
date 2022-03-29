// todo: create new file, insert the key in order to reduce the blanks
//
// Created by Machearn Ning on 3/21/22.
//

#ifndef MDBM_MDBM_H
#define MDBM_MDBM_H

#include "btree.h"

typedef struct {
    int idx_fd;
    int data_fd;
    Header* header;
    char* name;
}DB;

typedef struct {
    size_t size;
    char* data;
}Record;

void db_free_record(Record** record);

DB* db_open(const char* name, int oflag, ...);
void db_close(DB* db);

int db_fetch(DB* db, uint64_t key, Record* record);
int db_store(DB* db, uint64_t key, Record* record, int flag);
int db_delete(DB* db, uint64_t key);

int db_first_key(DB* db, Cell* cell);
int db_next_key(DB* db, uint64_t key, Cell* cell);

int db_reorganize(DB* db);

#define DB_INSERT 1
#define DB_REPLACE 2
#define DB_STORE 3

#endif //MDBM_MDBM_H
