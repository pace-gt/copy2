#pragma once

#include <lmdb.h>

struct DBISet {
    MDB_env *env;
    MDB_dbi inodeTable;
    MDB_dbi aclTable;
    MDB_dbi finfoByInodeTable;
    MDB_dbi pathToInodeTable;
};

struct Cursors {
    MDB_cursor *pathsCursor;
    MDB_cursor *finfoCursor;
    MDB_cursor *inodeInfoCursor;
    MDB_cursor *aclInfoCursor;
};

#ifdef __cplusplus
extern "C" {
#endif

int openDB(const char *path, struct DBISet *dbs);

int openCursors(struct DBISet *dbs, MDB_txn *txn, struct Cursors *cursors);

#ifdef __cplusplus
}
#endif
