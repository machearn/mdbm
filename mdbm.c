//
// Created by Machearn Ning on 3/21/22.
//

#include "mdbm.h"

static int lockRegion(int fd, int cmd, int type, off_t offset, int whence, off_t len);
static DB* dbAlloc(size_t nameLen);

static int lockRegion(int fd, int cmd, int type, off_t offset, int whence, off_t len) {
    struct flock lock;

    lock.l_type = (short) type;
    lock.l_start = offset;
    lock.l_whence = (short) whence;
    lock.l_len = len;

    return fcntl(fd, cmd, &lock);
}

#define readLock(fd, offset, whence, len) \
    lockRegion((fd), F_SETLK, F_RDLCK, (offset), (whence), (len))

#define readLockWait(fd, offset, whence, len) \
    lockRegion((fd), F_SETLKW, F_RDLCK, (offset), (whence), (len))

#define writeLock(fd, offset, whence, len) \
    lockRegion((fd), F_SETLK, F_WRLCK, (offset), (whence), (len))

#define writeLockWait(fd, offset, whence, len) \
    lockRegion((fd), F_SETLKW, F_WRLCK, (offset), (whence), (len))

#define unlock(fd, offset, whence, len) \
    lockRegion((fd), F_SETLK, F_UNLCK, (offset), (whence), (len))

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
