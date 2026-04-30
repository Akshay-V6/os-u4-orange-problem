// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }

    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print status
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;

    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }

    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;

    for (int i = 0; i < index->count; i++) {
        struct stat st;

        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }

    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;

    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;

        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 ||
                strcmp(ent->d_name, "..") == 0 ||
                strcmp(ent->d_name, ".pes") == 0 ||
                strcmp(ent->d_name, "pes") == 0 ||
                strstr(ent->d_name, ".o") != NULL) {
                continue;
            }

            int tracked = 0;

            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    tracked = 1;
                    break;
                }
            }

            if (!tracked) {
                struct stat st;
                stat(ent->d_name, &st);

                if (S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }

        closedir(dir);
    }

    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── HELPERS ────────────────────────────────────────────────────────────────

static int compare_entries(const void *a, const void *b) {
    const IndexEntry *ea = (const IndexEntry *)a;
    const IndexEntry *eb = (const IndexEntry *)b;
    return strcmp(ea->path, eb->path);
}

// ─── TODO IMPLEMENTED ───────────────────────────────────────────────────────

// Load index
int index_load(Index *index) {
    index->count = 0;

    FILE *fp = fopen(INDEX_FILE, "r");
    if (!fp) {
        return 0;
    }

    while (index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *entry = &index->entries[index->count];
        char hash_hex[HASH_HEX_SIZE + 1];

        int rc = fscanf(fp, "%o %64s %lu %u %511[^\n]\n",
                        &entry->mode,
                        hash_hex,
                        &entry->mtime_sec,
                        &entry->size,
                        entry->path);

        if (rc == EOF) break;

        if (rc != 5) {
            fclose(fp);
            return -1;
        }

        if (hex_to_hash(hash_hex, &entry->hash) != 0) {
            fclose(fp);
            return -1;
        }

        index->count++;
    }

    fclose(fp);
    return 0;
}

// Save index atomically
int index_save(const Index *index) {
    qsort((void *)index->entries,
          index->count,
          sizeof(IndexEntry),
          compare_entries);

    FILE *fp = fopen(".pes/index.tmp", "w");
    if (!fp) return -1;

    for (int i = 0; i < index->count; i++) {
        char hash_hex[HASH_HEX_SIZE + 1];

        hash_to_hex(&index->entries[i].hash, hash_hex);

        fprintf(fp, "%o %s %lu %u %s\n",
                index->entries[i].mode,
                hash_hex,
                index->entries[i].mtime_sec,
                index->entries[i].size,
                index->entries[i].path);
    }

    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    if (rename(".pes/index.tmp", INDEX_FILE) != 0) {
        return -1;
    }

    return 0;
}

// Add file to index
int index_add(Index *index, const char *path) {
    struct stat st;

    if (stat(path, &st) != 0) {
        fprintf(stderr, "error: cannot stat '%s'\n", path);
        return -1;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    void *data = malloc(st.st_size);

    if (!data) {
        fclose(fp);
        return -1;
    }

    if (fread(data, 1, st.st_size, fp) != (size_t)st.st_size) {
        free(data);
        fclose(fp);
        return -1;
    }

    fclose(fp);

    ObjectID hash;

    if (object_write(OBJ_BLOB, data, st.st_size, &hash) != 0) {
        free(data);
        return -1;
    }

    free(data);

    IndexEntry *entry = index_find(index, path);

    if (!entry) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index full\n");
            return -1;
        }

        entry = &index->entries[index->count++];
    }

    strcpy(entry->path, path);
    entry->hash = hash;
    entry->mode = st.st_mode;
    entry->mtime_sec = st.st_mtime;
    entry->size = st.st_size;

    return index_save(index);
}
