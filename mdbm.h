//
// Created by Machearn Ning on 3/21/22.
//

#ifndef MDBM_MDBM_H
#define MDBM_MDBM_H

#include <stdarg.h>
#include <errno.h>

#include "btree.h"
#include "lock.h"

typedef struct {
    int idxFd;
    int dataFd;
    Header* header;
    char* name;
}DB;

DB* dbOpen(const char* name, int oflag, ...);
void dbClose(DB* db);

void* dbFetch(DB* db, uint64_t key, size_t* dataLen);
int dbStore(DB* db, uint64_t key, void* data, int flag);
int dbDelete(DB* db, uint64_t key);

#endif //MDBM_MDBM_H
