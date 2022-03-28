//
// Created by Machearn Ning on 3/21/22.
//

#include <stdarg.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "mdbm.h"
#include "lock.h"

static DB* db_alloc(size_t nameLen) {
    Header* header = malloc(sizeof(Header));
    char* name = malloc(nameLen + 1);

    DB* db = malloc(sizeof(DB));
    db->header = header;
    db->name = name;

    return db;
}

static void db_free(DB** db) {
    if (!(*db)) return;
    if ((*db)->idx_fd >= 0) close((*db)->idx_fd);
    if ((*db)->data_fd >= 0) close((*db)->data_fd);
    free((*db)->header);
    free((*db)->name);
    free(*db);
    *db = NULL;
}

void db_free_record(Record** record) {
    if (!(*record)) return;
    free((*record)->data);
    free(*record);
    *record = NULL;
}

DB* db_open(const char* name, int oflag, ...) {
    size_t len;
    int mode;
    DB* db = NULL;

    len = strlen(name);
    db = db_alloc(len);
    strcpy(db->name, name);

    char* idx_file_name = malloc(len + 4 + 1);
    char* data_file_name = malloc(len + 4 + 1);
    strcpy(idx_file_name, name);
    strcat(idx_file_name, ".idx");
    strcpy(data_file_name, name);
    strcat(data_file_name, ".dat");

    if (oflag & O_CREAT) {
        va_list ap;
        va_start(ap, oflag);
        mode = va_arg(ap, int);
        va_end(ap);

        db->idx_fd = open(idx_file_name, oflag, mode);
        db->data_fd = open(data_file_name, oflag, mode);
    } else {
        db->idx_fd = open_index(idx_file_name, oflag, db->header);
        db->data_fd = open(data_file_name, oflag);
    }

    if (db->idx_fd < 0 || db->data_fd < 0) {
        db_free(&db);
        free(idx_file_name);
        free(data_file_name);
        return NULL;
    }

    if ((oflag & (O_CREAT | O_TRUNC)) == (O_CREAT | O_TRUNC)) {
        if (write_lock_wait(db->idx_fd, 0, SEEK_SET, 0) < 0) {
            db_free(&db);
            free(idx_file_name);
            free(data_file_name);
            return NULL;
        }
        if (create_tree(db->idx_fd) < 0) {
            db_free(&db);
            free(idx_file_name);
            free(data_file_name);
            return NULL;
        }
        if (unlock(db->idx_fd, 0, SEEK_SET, 0) < 0) {
            db_free(&db);
            free(idx_file_name);
            free(data_file_name);
            return NULL;
        }
    }

    free(idx_file_name);
    free(data_file_name);
    return db;
}

void db_close(DB* db) {
    db_free(&db);
}

int db_fetch(DB* db, uint64_t key, Record* record) {
    Cell* cell = malloc(sizeof(Cell));
    int pos = search(db->idx_fd, db->header, NULL, key, cell);
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

    if (read_lock_wait(db->data_fd, cell->offset, SEEK_SET, cell->size) < 0) {
        free(data);
        free(cell);
        errno = EAGAIN;
        return -1;
    }

    if (lseek(db->data_fd, cell->offset, SEEK_SET) < 0) {
        free(data);
        free(cell);
        return -1;
    }
    if (read(db->data_fd, data, cell->size) < 0) {
        free(data);
        free(cell);
        errno = EIO;
        return -1;
    }

    if (unlock(db->data_fd, cell->offset, SEEK_SET, cell->size) < 0) {
        free(data);
        free(cell);
        errno = EAGAIN;
        return -1;
    }

    record->size = cell->size;
    record->data = data;
    return 0;
}

int db_store(DB* db, uint64_t key, Record* record, int flag) {
    if (record == NULL || record->data == NULL) {
        errno = EINVAL;
        return -1;
    }

    if (flag != DB_INSERT && flag != DB_REPLACE && flag != DB_STORE) {
        errno = EINVAL;
        return -1;
    }

    Page* node = malloc_page();
    Cell* new_cell = malloc_cell();
    Cell* old_cell = malloc_cell();

    new_cell->size = record->size;
    new_cell->key = key;
    if ((new_cell->offset = lseek(db->data_fd, 0, SEEK_END)) < 0) {
        free_page(&node);
        free_cell(&new_cell);
        free_cell(&old_cell);
        errno = EIO;
        return -1;
    }

    int pos = search(db->idx_fd, db->header, node, key, old_cell);
    if (pos < 0) {
        free_page(&node);
        free_cell(&new_cell);
        free_cell(&old_cell);
        errno = ENOENT;
        return -1;
    }
    if (node->cells[pos].key == key) {
        if (flag == DB_INSERT) {
            free_page(&node);
            free_cell(&new_cell);
            free_cell(&old_cell);
            errno = EEXIST;
            return -1;
        }

        ssize_t ret = update(db->idx_fd, node, pos, new_cell);
        free_page(&node);
        if (ret < 0) {
            free_cell(&new_cell);
            free_cell(&old_cell);
            errno = EAGAIN;
            return -1;
        }

        if (write_lock_wait(db->data_fd, node->cells[pos].offset, SEEK_SET, node->cells[pos].size) <
            0) {
            free_cell(&new_cell);
            free_cell(&old_cell);
            errno = EAGAIN;
            return -1;
        }

        if (lseek(db->data_fd, node->cells[pos].offset, SEEK_SET) < 0) {
            free_cell(&new_cell);
            free_cell(&old_cell);
            errno = EIO;
            return -1;
        }

        if (old_cell->size < record->size) {
            char* blank = malloc(old_cell->size);
            if (blank == NULL) {
                free_cell(&new_cell);
                free_cell(&old_cell);
                errno = ENOMEM;
                return -1;
            }
            if (write(db->data_fd, blank, old_cell->size) < 0) {
                free(blank);
                free_cell(&new_cell);
                free_cell(&old_cell);
                errno = EIO;
                return -1;
            }
            free(blank);
            if (lseek(db->data_fd, 0, SEEK_END) < 0) {
                free_cell(&new_cell);
                free_cell(&old_cell);
                errno = EIO;
                return -1;
            }
        }

        if (write(db->data_fd, record->data, record->size) < 0) {
            free_page(&node);
            free_cell(&new_cell);
            errno = EIO;
            return -1;
        }

        if (unlock(db->data_fd, node->cells[pos].offset, SEEK_SET, node->cells[pos].size) < 0) {
            free_page(&node);
            free_cell(&new_cell);
            errno = EAGAIN;
            return -1;
        }
    } else {
        free_cell(&old_cell);
        if (flag == DB_REPLACE) {
            free_page(&node);
            free_cell(&new_cell);
            errno = ENOENT;
            return -1;
        }
        ssize_t ret = insert(db->idx_fd, db->header, node, pos, new_cell);
        free_page(&node);
        if (ret < 0) {
            free_cell(&new_cell);
            errno = EAGAIN;
            return -1;
        }

        if (write_lock_wait(db->data_fd, new_cell->offset, SEEK_SET, new_cell->size) < 0) {
            free_cell(&new_cell);
            errno = EAGAIN;
            return -1;
        }
        if (lseek(db->data_fd, new_cell->offset, SEEK_SET) < 0) {
            free_cell(&new_cell);
            errno = EIO;
            return -1;
        }
        if (write(db->data_fd, record->data, record->size) < 0) {
            free_cell(&new_cell);
            errno = EIO;
            return -1;
        }
        if (unlock(db->data_fd, new_cell->offset, SEEK_SET, new_cell->size) < 0) {
            free_cell(&new_cell);
            errno = EAGAIN;
            return -1;
        }
    }

    free_page(&node);
    free_cell(&new_cell);
    free_cell(&old_cell);
    return 0;
}

int db_delete(DB* db, uint64_t key) {
    Page* node = malloc_page();
    if (node == NULL) {
        errno = ENOMEM;
        return -1;
    }

    Cell* cell = malloc_cell();
    if (cell == NULL) {
        free_page(&node);
        errno = ENOMEM;
        return -1;
    }

    int pos = search(db->idx_fd, db->header, node, key, cell);
    if (pos < 0) {
        free_page(&node);
        free_cell(&cell);
        errno = ENOENT;
        return -1;
    }
    if (node->cells[pos].key != key) {
        free_page(&node);
        free_cell(&cell);
        errno = ENOENT;
        return -1;
    }

    ssize_t ret = delete(db->idx_fd, node, pos);
    free_page(&node);
    if (ret < 0) {
        free_cell(&cell);
        errno = ENOENT;
        return -1;
    }

    if (write_lock_wait(db->data_fd, cell->offset, SEEK_SET, cell->size) < 0) {
        free_cell(&cell);
        errno = EAGAIN;
        return -1;
    }

    if (lseek(db->data_fd, cell->offset, SEEK_SET) < 0) {
        free_cell(&cell);
        return -1;
    }
    char* blank = malloc(cell->size);
    if (blank == NULL) {
        free_cell(&cell);
        errno = ENOMEM;
        return -1;
    }
    memset(blank, 0, cell->size);

    ret = write(db->data_fd, blank, cell->size);
    free(blank);
    blank = NULL;
    free_cell(&cell);
    if (ret < 0) {
        errno = EIO;
        return -1;
    }

    if (unlock(db->data_fd, cell->offset, SEEK_SET, cell->size) < 0) {
        errno = EAGAIN;
        return -1;
    }
    return 0;
}
