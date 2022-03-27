//
// Created by Machearn Ning on 3/21/22.
//

#ifndef MDBM_MDBM_H
#define MDBM_MDBM_H

#include "btree.h"

typedef struct {
    int idxFd;
    int dataFd;
    Header* header;
    char* name;
}DB;

typedef struct {
    size_t size;
    char* data;
}Record;

void dbFreeRecord(Record** record);

DB* dbOpen(const char* name, int oflag, ...);
void dbClose(DB* db);

int dbFetch(DB* db, uint64_t key, Record* record);
int dbStore(DB* db, uint64_t key, Record* record, int flag);
int dbDelete(DB* db, uint64_t key);

#define DB_INSERT 1
#define DB_REPLACE 2
#define DB_STORE 3

#endif //MDBM_MDBM_H
