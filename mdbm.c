//
// Created by Machearn Ning on 3/21/22.
//

#include "mdbm.h"

static DB* dbAlloc(size_t nameLen);

static DB* dbAlloc(size_t nameLen) {
    Header* header = malloc(sizeof(Header));
    char* name = malloc(nameLen + 1);

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

void dbFreeRecord(Record** record) {
    if (!(*record)) return;
    free((*record)->data);
    free(*record);
    *record = NULL;
}

DB* dbOpen(const char* name, int oflag, ...) {
    size_t len;
    int mode;
    DB* db = NULL;

    len = strlen(name);
    db = dbAlloc(len);
    strcpy(db->name, name);

    char* idxName = malloc(len + 4 + 1);
    char* dataName = malloc(len + 4 + 1);
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

int dbFetch(DB* db, uint64_t key, Record* record) {
    Cell* cell = malloc(sizeof(Cell));
    int pos = search(db->idxFd, db->header, NULL, key, cell);
    if (pos < 0 || cell->key != key) {
        errno = ENOENT;
        return -1;
    }

    void* data = malloc(cell->size);
    if (data == NULL) {
        free(cell);
        errno = ENOMEM;
        return -1;
    }

    if (readLockWait(db->dataFd, cell->offset, SEEK_SET, cell->size) < 0) {
        free(data);
        free(cell);
        errno = EAGAIN;
        return -1;
    }

    if (lseek(db->dataFd, cell->offset, SEEK_SET) < 0) {
        free(data);
        free(cell);
        return -1;
    }
    if (read(db->dataFd, data, cell->size) < 0) {
        free(data);
        free(cell);
        errno = EIO;
        return -1;
    }

    if (unlock(db->dataFd, cell->offset, SEEK_SET, cell->size) < 0) {
        free(data);
        free(cell);
        errno = EAGAIN;
        return -1;
    }

    record->size = cell->size;
    record->data = data;
    return 0;
}

int dbStore(DB* db, uint64_t key, Record* record, int flag) {
    if (record == NULL || record->data == NULL) {
        errno = EINVAL;
        return -1;
    }

    if (flag != DB_INSERT && flag != DB_REPLACE && flag != DB_STORE) {
        errno = EINVAL;
        return -1;
    }

    Page* node = mallocPage();
    Cell* newCell = mallocCell();
    Cell* oldCell = mallocCell();

    newCell->size = record->size;
    newCell->key = key;
    if ((newCell->offset = lseek(db->dataFd, 0, SEEK_END)) < 0) {
        freePage(&node);
        freeCell(&newCell);
        freeCell(&oldCell);
        errno = EIO;
        return -1;
    }

    int pos = search(db->idxFd, db->header, node, key, oldCell);
    if (pos < 0) {
        freePage(&node);
        freeCell(&newCell);
        freeCell(&oldCell);
        errno = ENOENT;
        return -1;
    }
    if (node->cells[pos].key == key) {
        if (flag == DB_INSERT) {
            freePage(&node);
            freeCell(&newCell);
            freeCell(&oldCell);
            errno = EEXIST;
            return -1;
        }

        ssize_t ret = update(db->idxFd, node, pos, newCell);
        freePage(&node);
        if (ret < 0) {
            freeCell(&newCell);
            freeCell(&oldCell);
            errno = EAGAIN;
            return -1;
        }

        if (writeLockWait(db->dataFd, node->cells[pos].offset, SEEK_SET, node->cells[pos].size) <
            0) {
            freeCell(&newCell);
            freeCell(&oldCell);
            errno = EAGAIN;
            return -1;
        }

        if (lseek(db->dataFd, node->cells[pos].offset, SEEK_SET) < 0) {
            freeCell(&newCell);
            freeCell(&oldCell);
            errno = EIO;
            return -1;
        }

        if (oldCell->size < record->size) {
            char* blank = malloc(oldCell->size);
            if (blank == NULL) {
                freeCell(&newCell);
                freeCell(&oldCell);
                errno = ENOMEM;
                return -1;
            }
            if (write(db->dataFd, blank, oldCell->size) < 0) {
                free(blank);
                freeCell(&newCell);
                freeCell(&oldCell);
                errno = EIO;
                return -1;
            }
            free(blank);
            if (lseek(db->dataFd, 0, SEEK_END) < 0) {
                freeCell(&newCell);
                freeCell(&oldCell);
                errno = EIO;
                return -1;
            }
        }

        if (write(db->dataFd, record->data, record->size) < 0) {
            freePage(&node);
            freeCell(&newCell);
            errno = EIO;
            return -1;
        }

        if (unlock(db->dataFd, node->cells[pos].offset, SEEK_SET, node->cells[pos].size) < 0) {
            freePage(&node);
            freeCell(&newCell);
            errno = EAGAIN;
            return -1;
        }
    } else {
        freeCell(&oldCell);
        if (flag == DB_REPLACE) {
            freePage(&node);
            freeCell(&newCell);
            errno = ENOENT;
            return -1;
        }
        ssize_t ret = insert(db->idxFd, db->header, node, pos, newCell);
        freePage(&node);
        if (ret < 0) {
            freeCell(&newCell);
            errno = EAGAIN;
            return -1;
        }

        if (writeLockWait(db->dataFd, newCell->offset, SEEK_SET, newCell->size) < 0) {
            freeCell(&newCell);
            errno = EAGAIN;
            return -1;
        }
        if (lseek(db->dataFd, newCell->offset, SEEK_SET) < 0) {
            freeCell(&newCell);
            errno = EIO;
            return -1;
        }
        if (write(db->dataFd, record->data, record->size) < 0) {
            freeCell(&newCell);
            errno = EIO;
            return -1;
        }
        if (unlock(db->dataFd, newCell->offset, SEEK_SET, newCell->size) < 0) {
            freeCell(&newCell);
            errno = EAGAIN;
            return -1;
        }
    }

    freePage(&node);
    freeCell(&newCell);
    freeCell(&oldCell);
    return 0;
}

int dbDelete(DB* db, uint64_t key) {
    Page* node = mallocPage();
    if (node == NULL) {
        errno = ENOMEM;
        return -1;
    }

    Cell* cell = mallocCell();
    if (cell == NULL) {
        freePage(&node);
        errno = ENOMEM;
        return -1;
    }

    int pos = search(db->idxFd, db->header, node, key, cell);
    if (pos < 0) {
        freePage(&node);
        freeCell(&cell);
        errno = ENOENT;
        return -1;
    }
    if (node->cells[pos].key != key) {
        freePage(&node);
        freeCell(&cell);
        errno = ENOENT;
        return -1;
    }

    ssize_t ret = delete(db->idxFd, node, pos);
    freePage(&node);
    if (ret < 0) {
        freeCell(&cell);
        errno = ENOENT;
        return -1;
    }

    if (writeLockWait(db->dataFd, cell->offset, SEEK_SET, cell->size) < 0) {
        freeCell(&cell);
        errno = EAGAIN;
        return -1;
    }

    if (lseek(db->dataFd, cell->offset, SEEK_SET) < 0) {
        freeCell(&cell);
        return -1;
    }
    char* blank = malloc(cell->size);
    if (blank == NULL) {
        freeCell(&cell);
        errno = ENOMEM;
        return -1;
    }
    memset(blank, 0, cell->size);

    ret = write(db->dataFd, blank, cell->size);
    free(blank);
    blank = NULL;
    freeCell(&cell);
    if (ret < 0) {
        errno = EIO;
        return -1;
    }

    if (unlock(db->dataFd, cell->offset, SEEK_SET, cell->size) < 0) {
        errno = EAGAIN;
        return -1;
    }
    return 0;
}
