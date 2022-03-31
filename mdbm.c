//
// Created by Machearn Ning on 3/21/22.
//

#include <stdarg.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/syslimits.h>
#include <libgen.h>
#include <stdio.h>

#include "mdbm.h"
#include "lock.h"

Record* malloc_record() {
    Record* record = NULL;
    record = (Record*) malloc(sizeof(Record));
    memset(record, 0, sizeof(Record));
    return record;
}

void free_record(Record** ptr) {
    if (ptr == NULL) return;
    free(*ptr);
    *ptr = NULL;
}

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
        db->idx_fd = open(idx_file_name, oflag);
        load_index_header(db->idx_fd, db->header);
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

int db_first_key(DB* db, Cell* cell) {
    Page* leaf = malloc_page();
    if (leaf == NULL) {
        errno = ENOMEM;
        return -1;
    }
    return first_key(db->idx_fd, db->header, leaf, cell);
}

int db_next_key(DB* db, Page* leaf, int* pos, Cell* cell) {
    return next_key(db->idx_fd, leaf, pos, cell);
}

int db_reorganize(DB* db) {
    char* tmp_idx_path = "/tmp/tmp.idx";
    int new_idx_fd = open(tmp_idx_path, O_RDWR | O_CREAT, 0644);
    if (new_idx_fd < 0) {
        return errno;
    }

    char* tmp_data_path = "/tmp/tmp.data";
    int new_data_fd = open(tmp_data_path, O_RDWR | O_CREAT, 0644);
    if (new_data_fd < 0) {
        close(new_idx_fd);
        return errno;
    }

    Cell* cell = malloc_cell();
    if (cell == NULL) {
        close(new_idx_fd);
        close(new_data_fd);
        errno = ENOMEM;
        return -1;
    }

    Record* record = malloc_record();
    if (record == NULL) {
        close(new_idx_fd);
        close(new_data_fd);
        free_cell(&cell);
        errno = ENOMEM;
        return -1;
    }

    Page* leaf = malloc_page();
    if (leaf == NULL) {
        close(new_idx_fd);
        close(new_data_fd);
        free_cell(&cell);
        free_record(&record);
        errno = ENOMEM;
        return -1;
    }

    Page* new_leaf = malloc_page();
    if (new_leaf == NULL) {
        close(new_idx_fd);
        close(new_data_fd);
        free_cell(&cell);
        free_record(&record);
        free_page(&leaf);
        errno = ENOMEM;
        return -1;
    }

    int pos = -1;
    int new_pos = -1;
    int next_key_ret = 0;
    off_t new_record_offset = 0;
    Header new_header;

    if (first_key(db->idx_fd, db->header, leaf, cell) < 0) {
        close(new_idx_fd);
        close(new_data_fd);
        free_cell(&cell);
        free_record(&record);
        free_page(&leaf);
        free_page(&new_leaf);
        errno = ENOENT;
        return -1;
    }
    pos++;

    create_tree(new_idx_fd);
    load_index_header(new_idx_fd, &new_header);
    get_left_most_leaf(db->idx_fd, &new_header, new_leaf);

    do {
        if (read_lock_wait(db->data_fd, cell->offset, SEEK_SET, cell->size) < 0) {
            close(new_idx_fd);
            close(new_data_fd);
            free_cell(&cell);
            free_record(&record);
            free_page(&leaf);
            free_page(&new_leaf);
            errno = EAGAIN;
            return -1;
        }
        if (lseek(db->data_fd, cell->offset, SEEK_SET) < 0) {
            close(new_idx_fd);
            close(new_data_fd);
            free_cell(&cell);
            free_record(&record);
            free_page(&leaf);
            free_page(&new_leaf);
            return -1;
        }
        if (read(db->data_fd, record, cell->size) < 0) {
            close(new_idx_fd);
            close(new_data_fd);
            free_cell(&cell);
            free_record(&record);
            free_page(&leaf);
            free_page(&new_leaf);
            errno = EIO;
            return -1;
        }
        if (unlock(db->data_fd, cell->offset, SEEK_SET, cell->size) < 0) {
            close(new_idx_fd);
            close(new_data_fd);
            free_cell(&cell);
            free_record(&record);
            free_page(&leaf);
            free_page(&new_leaf);
            errno = EAGAIN;
            return -1;
        }

        if (write_lock_wait(new_data_fd, new_record_offset, SEEK_SET, cell->size) < 0) {
            close(new_idx_fd);
            close(new_data_fd);
            free_cell(&cell);
            free_record(&record);
            free_page(&leaf);
            free_page(&new_leaf);
            errno = EAGAIN;
            return -1;
        }
        if (write(new_data_fd, record, cell->size) < 0) {
            close(new_idx_fd);
            close(new_data_fd);
            free_cell(&cell);
            free_record(&record);
            free_page(&leaf);
            free_page(&new_leaf);
            errno = EIO;
            return -1;
        }
        new_record_offset += (off_t) cell->size;
        if (unlock(new_data_fd, 0, SEEK_SET, cell->size) < 0) {
            close(new_idx_fd);
            close(new_data_fd);
            free_cell(&cell);
            free_record(&record);
            free_page(&leaf);
            free_page(&new_leaf);
            errno = EAGAIN;
            return -1;
        }

        new_pos = search(new_idx_fd, &new_header, new_leaf, cell->key, NULL);
        insert(new_idx_fd, &new_header, new_leaf, new_pos, cell);

        next_key_ret = db_next_key(db, leaf, &pos, cell);
        if (next_key_ret == -1) {
            close(new_idx_fd);
            close(new_data_fd);
            free_cell(&cell);
            free_record(&record);
            free_page(&leaf);
            free_page(&new_leaf);
            errno = EIO;
            return -1;
        }
    } while (next_key_ret == -2);

    char path[PATH_MAX];
    if (fcntl(db->idx_fd, F_GETPATH, path) < 0) {
        close(new_idx_fd);
        close(new_data_fd);
        free_cell(&cell);
        free_record(&record);
        free_page(&leaf);
        free_page(&new_leaf);
        errno = EIO;
        return -1;
    }
    close(db->idx_fd);
    db->idx_fd = new_idx_fd;
    if (rename(tmp_idx_path, path) < 0) {
        close(new_idx_fd);
        close(new_data_fd);
        free_cell(&cell);
        free_record(&record);
        free_page(&leaf);
        free_page(&new_leaf);
        errno = EIO;
        return -1;
    }

    if (fcntl(db->data_fd, F_GETPATH, path) < 0) {
        close(new_idx_fd);
        close(new_data_fd);
        free_cell(&cell);
        free_record(&record);
        free_page(&leaf);
        free_page(&new_leaf);
        errno = EIO;
        return -1;
    }
    close(db->data_fd);
    db->data_fd = new_data_fd;
    if (rename(tmp_data_path, path) < 0) {
        close(new_idx_fd);
        close(new_data_fd);
        free_cell(&cell);
        free_record(&record);
        free_page(&leaf);
        free_page(&new_leaf);
        errno = EIO;
        return -1;
    }

    return 0;
}
