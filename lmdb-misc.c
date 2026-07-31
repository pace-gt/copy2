#include "lmdb-misc.h"
#include "lmdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int openDB(const char *path, struct DBISet *dbs) {

    MDB_env *mdbEnv = NULL;
    int err;
    if ((err = mdb_env_create(&mdbEnv))) {
        fprintf(stderr, "ERROR: failed to create mdb env: error %s\n",
                mdb_strerror(err));
        return EXIT_FAILURE;
    }
    mdb_env_set_maxdbs(mdbEnv, 8);
    mdb_env_set_mapsize(mdbEnv, (size_t)1048576 * (size_t)10000);
    if ((err = mdb_env_open(mdbEnv, path,
                            MDB_NOTLS | MDB_NOLOCK | MDB_CREATE | MDB_NOSUBDIR,
                            0777))) {
        fprintf(stderr, "ERROR: failed to open mdb environment: error %s\n",
                mdb_strerror(err));
        return EXIT_FAILURE;
    }

    MDB_dbi inodeTable;
    MDB_dbi aclTable;
    MDB_dbi finfoByInodeTable;
    MDB_dbi pathToInodeTable;
    {
        MDB_txn *txn;
        if ((err = mdb_txn_begin(mdbEnv, NULL, 0, &txn))) {
            fprintf(stderr,
                    "ERROR: failed to begin mdb dbi aquisition transaction: "
                    "error %s",
                    mdb_strerror(err));
            return EXIT_FAILURE;
        }

        if ((err = mdb_dbi_open(txn, "INODES",
                                MDB_CREATE | MDB_INTEGERKEY | MDB_DUPSORT |
                                    MDB_DUPFIXED,
                                &inodeTable))) {
            fprintf(stderr, "ERROR: failed to get/create INODES table: %s\n",
                    mdb_strerror(err));
            return EXIT_FAILURE;
        }

        if ((err = mdb_dbi_open(txn, "ACLS", MDB_CREATE, &aclTable))) {
            fprintf(stderr, "ERROR: failed to get/create INODES table: %s\n",
                    mdb_strerror(err));
            return EXIT_FAILURE;
        }

        if ((err = mdb_dbi_open(txn, "FINFOBYINODE", MDB_CREATE | MDB_DUPSORT,
                                &finfoByInodeTable))) {
            fprintf(stderr, "ERROR: failed to get/create FILES table: %s\n",
                    mdb_strerror(err));
            return EXIT_FAILURE;
        }

        if ((err = mdb_dbi_open(txn, "PATHTOINODE", MDB_CREATE,
                                &pathToInodeTable))) {
            fprintf(stderr,
                    "ERROR: failed to get/create PATHTOINODE table: %s\n",
                    mdb_strerror(err));
            return EXIT_FAILURE;
        }

        if ((err = mdb_txn_commit(txn))) {
            fprintf(stderr,
                    "ERROR: failed to commit mdb dbi aquisition transaction: "
                    "error %s",
                    strerror(err));
            return EXIT_FAILURE;
        }
    }

    dbs->env = mdbEnv;
    dbs->inodeTable = inodeTable;
    dbs->aclTable = aclTable;
    dbs->finfoByInodeTable = finfoByInodeTable;
    dbs->pathToInodeTable = pathToInodeTable;

    return 0;
}

int openCursors(struct DBISet *dbs, MDB_txn *txn, struct Cursors *cursors) {
    int err;
    if ((err = mdb_cursor_open(txn, dbs->pathToInodeTable,
                               &cursors->pathsCursor))) {
        fprintf(stderr, "ERROR: failed to open paths cursor: %s\n",
                mdb_strerror(err));
        return EXIT_FAILURE;
    }

    if ((err = mdb_cursor_open(txn, dbs->finfoByInodeTable,
                               &cursors->finfoCursor))) {
        fprintf(stderr, "ERROR: failed to open finfo cursor: %s\n",
                mdb_strerror(err));
        return EXIT_FAILURE;
    }

    if ((err = mdb_cursor_open(txn, dbs->inodeTable,
                               &cursors->inodeInfoCursor))) {
        fprintf(stderr, "ERROR: failed to open inodeinfo cursor: %s\n",
                mdb_strerror(err));
        return EXIT_FAILURE;
    }

    if ((err = mdb_cursor_open(txn, dbs->aclTable, &cursors->aclInfoCursor))) {
        fprintf(stderr, "ERROR: failed to open aclinfo cursor: %s\n",
                mdb_strerror(err));
        return EXIT_FAILURE;
    }
    return 0;
}

