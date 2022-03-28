//
// Created by Machearn Ning on 3/24/22.
//

#include "lock.h"

int lock_region(int fd, int cmd, int type, off_t offset, int whence, off_t len) {
    struct flock lock;

    lock.l_type = (short) type;
    lock.l_start = offset;
    lock.l_whence = (short) whence;
    lock.l_len = len;

    return fcntl(fd, cmd, &lock);
}

