//
// Created by Machearn Ning on 3/22/22.
//

#ifndef MDBM_LOCK_H
#define MDBM_LOCK_H

#include <sys/types.h>
#include <fcntl.h>

int lockRegion(int fd, int cmd, int type, off_t offset, int whence, off_t len);

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

#endif //MDBM_LOCK_H
