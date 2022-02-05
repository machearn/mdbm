//
// Created by Machearn Ning on 2/4/22.
//

#include "btree.h"

Page* mallocPage() {
    Page* p;
    p = (Page*) malloc(4096);
    memset(p, 0, 4096);
    return p;
}

int loadPage(int fd, off_t offset, Page** pPage) {
    if (lseek(fd, offset, SEEK_SET) < 0) {
        return -1;
    }
    if (read(fd, *pPage, 4096) < 0) {
        return -1;
    }
    return 0;
}
