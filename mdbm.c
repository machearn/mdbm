//
// Created by Machearn Ning on 3/21/22.
//

#include "mdbm.h"

static DB* dbAlloc(size_t nameLen);

static DB* dbAlloc(size_t nameLen) {
    Header* header = malloc(sizeof(Header));
    char* name = malloc(nameLen+1);

    DB* db = malloc(sizeof(DB));
    db->header = header;
    db->name = name;

    return db;
}

static void dbFree(DB** db) {
    if (!(*db)) return;
    if ((*db)->idxFd >= 0) close((*db)->idxFd);
    if ((*db)->dataFd >= 0) close((*db)->dataFd);
    free((*db)->header);
    free((*db)->name);
    free(*db);
    *db = NULL;
}

DB* dbOpen(const char* name, int oflag, ...) {
    size_t len;
    int mode;
    DB* db = NULL;

    len = strlen(name);
    db = dbAlloc(len);
    strcpy(db->name, name);

    char* idxName = malloc(len+4+1);
    char* dataName = malloc(len+4+1);
    strcpy(idxName, name);
    strcat(idxName, ".idx");
    strcpy(dataName, name);
    strcat(dataName, ".dat");

    if (oflag & O_CREAT) {
        va_list ap;
        va_start(ap, oflag);
        mode = va_arg(ap, int);
        va_end(ap);

        db->idxFd = open(idxName, oflag, mode);
        db->dataFd = open(dataName, oflag, mode);
    } else {
        db->idxFd = openIndex(idxName, oflag, db->header);
        db->dataFd = open(dataName, oflag);
    }

    if (db->idxFd < 0 || db->dataFd < 0) {
        dbFree(&db);
        free(idxName);
        free(dataName);
        return NULL;
    }

    if ((oflag & (O_CREAT | O_TRUNC)) == (O_CREAT | O_TRUNC)) {
        if (writeLockWait(db->idxFd, 0, SEEK_SET, 0) < 0) {
            dbFree(&db);
            free(idxName);
            free(dataName);
            return NULL;
        }
        if (createTree(db->idxFd) < 0) {
            dbFree(&db);
            free(idxName);
            free(dataName);
            return NULL;
        }
        if (unlock(db->idxFd, 0, SEEK_SET, 0) < 0) {
            dbFree(&db);
            free(idxName);
            free(dataName);
            return NULL;
        }
    }

    free(idxName);
    free(dataName);
    return db;
}

void dbClose(DB* db) {
    dbFree(&db);
}

void* dbFetch(DB* db, uint64_t key, size_t* dataLen) {
    Cell* cell = malloc(sizeof(Cell));
    int pos = search(db->idxFd, db->header, NULL, key, cell);
    if (pos < 0 || cell->key != key) {
        errno = ENOENT;
        return NULL;
    }

    lseek(db->dataFd, cell->offset, SEEK_SET);
    if (writeLockWait(db->dataFd, cell->offset, SEEK_SET, sizeof(size_t)) < 0) {
        free(cell);
        errno = EAGAIN;
        return NULL;
    }
    if (read(db->dataFd, dataLen, sizeof(size_t)) < 0) {
        free(cell);
        errno = EIO;
        return NULL;
    }
    if (unlock(db->dataFd, cell->offset, SEEK_SET, sizeof(size_t)) < 0) {
        free(cell);
        errno = EAGAIN;
        return NULL;
    }

    void* data = malloc(*dataLen);
    if (data == NULL) {
        free(cell);
        errno = ENOMEM;
        return NULL;
    }
    if (writeLockWait(db->dataFd, cell->offset+sizeof(size_t), SEEK_SET, *dataLen) < 0) {
        free(data);
        free(cell);
        errno = EAGAIN;
        return NULL;
    }
    if (read(db->dataFd, data, *dataLen) < 0) {
        free(data);
        free(cell);
        errno = EIO;
        return NULL;
    }
    if (unlock(db->dataFd, cell->offset+sizeof(size_t), SEEK_SET, *dataLen) < 0) {
        free(data);
        free(cell);
        errno = EAGAIN;
        return NULL;
    }
    return data;
}
