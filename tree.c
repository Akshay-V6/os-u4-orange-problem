// tree.c — Tree object serialization and construction

#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <inttypes.h>

// Forward declaration
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;

        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;

        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1;

        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;

        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';

        ptr = null_byte + 1;

        if (ptr + HASH_SIZE > end) return -1;

        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }

    return 0;
}

static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name,
                  ((const TreeEntry *)b)->name);
}

int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 296;

    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    Tree sorted_tree = *tree;

    qsort(sorted_tree.entries,
          sorted_tree.count,
          sizeof(TreeEntry),
          compare_tree_entries);

    size_t offset = 0;

    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];

        int written = sprintf((char *)buffer + offset,
                              "%o %s",
                              entry->mode,
                              entry->name);

        offset += written + 1;

        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;

    return 0;
}

// ─── FIXED tree_from_index ──────────────────────────────────────────────────

int tree_from_index(ObjectID *id_out) {
    FILE *fp = fopen(".pes/index", "r");
    if (!fp) return -1;

    Tree tree;
    memset(&tree, 0, sizeof(tree));

    char line[1024];

    while (fgets(line, sizeof(line), fp)) {
        if (tree.count >= MAX_TREE_ENTRIES) break;

        uint32_t mode;
        char hash_hex[HASH_HEX_SIZE + 1];
        uint64_t mtime_sec;
        uint32_t size;
        char path[512];

        if (sscanf(line,
                   "%o %64s %" SCNu64 " %u %511[^\n]",
                   &mode,
                   hash_hex,
                   &mtime_sec,
                   &size,
                   path) != 5) {
            continue;
        }

        TreeEntry *entry = &tree.entries[tree.count];

        if (hex_to_hash(hash_hex, &entry->hash) != 0) {
            continue;
        }

        const char *filename = strrchr(path, '/');
        filename = filename ? filename + 1 : path;

        strncpy(entry->name, filename, sizeof(entry->name) - 1);
        entry->name[sizeof(entry->name) - 1] = '\0';

        entry->mode = mode;

        tree.count++;
    }

    fclose(fp);

    if (tree.count == 0) {
        return -1;
    }

    void *serialized;
    size_t serialized_len;

    if (tree_serialize(&tree, &serialized, &serialized_len) != 0) {
        return -1;
    }

    int rc = object_write(OBJ_TREE,
                          serialized,
                          serialized_len,
                          id_out);

    free(serialized);

    return rc;
}
