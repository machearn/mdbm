//
// Created by Machearn Ning on 3/22/22.
//

#ifndef MDBM_LOCK_H
#define MDBM_LOCK_H

#include <sys/types.h>
#include <fcntl.h>

int lock_region(int fd, int cmd, int type, off_t offset, int whence, off_t len);

#define read_lock(fd, offset, whence, len) \
    lock_region((fd), F_SETLK, F_RDLCK, (offset), (whence), (len))

#define read_lock_wait(fd, offset, whence, len) \
    lock_region((fd), F_SETLKW, F_RDLCK, (offset), (whence), (len))

#define write_lock(fd, offset, whence, len) \
    lock_region((fd), F_SETLK, F_WRLCK, (offset), (whence), (len))

#define write_lock_wait(fd, offset, whence, len) \
    lock_region((fd), F_SETLKW, F_WRLCK, (offset), (whence), (len))

#define unlock(fd, offset, whence, len) \
    lock_region((fd), F_SETLK, F_UNLCK, (offset), (whence), (len))

#endif //MDBM_LOCK_H
