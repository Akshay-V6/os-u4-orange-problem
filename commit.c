// commit.c — Commit creation and history traversal

#include "commit.h"
#include "index.h"
#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

// Forward declarations
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

int commit_parse(const void *data, size_t len, Commit *commit_out) {
    (void)len;

    const char *p = (const char *)data;
    char hex[HASH_HEX_SIZE + 1];

    if (sscanf(p, "tree %64s\n", hex) != 1) return -1;
    if (hex_to_hash(hex, &commit_out->tree) != 0) return -1;

    p = strchr(p, '\n');
    if (!p) return -1;
    p++;

    if (strncmp(p, "parent ", 7) == 0) {
        if (sscanf(p, "parent %64s\n", hex) != 1) return -1;
        if (hex_to_hash(hex, &commit_out->parent) != 0) return -1;

        commit_out->has_parent = 1;

        p = strchr(p, '\n');
        if (!p) return -1;
        p++;
    } else {
        commit_out->has_parent = 0;
    }

    char author_buf[256];
    uint64_t ts;

    if (sscanf(p, "author %255[^\n]\n", author_buf) != 1) return -1;

    char *last_space = strrchr(author_buf, ' ');
    if (!last_space) return -1;

    ts = (uint64_t)strtoull(last_space + 1, NULL, 10);
    *last_space = '\0';

    snprintf(commit_out->author, sizeof(commit_out->author), "%s", author_buf);
    commit_out->timestamp = ts;

    p = strchr(p, '\n');
    if (!p) return -1;
    p++;

    p = strchr(p, '\n');
    if (!p) return -1;
    p++;

    p = strchr(p, '\n');
    if (!p) return -1;
    p++;

    snprintf(commit_out->message, sizeof(commit_out->message), "%s", p);

    return 0;
}

int commit_serialize(const Commit *commit, void **data_out, size_t *len_out) {
    char tree_hex[HASH_HEX_SIZE + 1];
    char parent_hex[HASH_HEX_SIZE + 1];

    hash_to_hex(&commit->tree, tree_hex);

    char buf[8192];
    int n = 0;

    n += snprintf(buf + n, sizeof(buf) - n, "tree %s\n", tree_hex);

    if (commit->has_parent) {
        hash_to_hex(&commit->parent, parent_hex);
        n += snprintf(buf + n, sizeof(buf) - n, "parent %s\n", parent_hex);
    }

    n += snprintf(buf + n, sizeof(buf) - n,
                  "author %s %" PRIu64 "\n"
                  "committer %s %" PRIu64 "\n"
                  "\n"
                  "%s",
                  commit->author,
                  commit->timestamp,
                  commit->author,
                  commit->timestamp,
                  commit->message);

    *data_out = malloc(n + 1);
    if (!*data_out) return -1;

    memcpy(*data_out, buf, n + 1);
    *len_out = (size_t)n;

    return 0;
}

int commit_walk(commit_walk_fn callback, void *ctx) {
    ObjectID id;

    if (head_read(&id) != 0) return -1;

    while (1) {
        ObjectType type;
        void *raw;
        size_t raw_len;

        if (object_read(&id, &type, &raw, &raw_len) != 0) {
            return -1;
        }

        Commit c;
        int rc = commit_parse(raw, raw_len, &c);

        free(raw);

        if (rc != 0) return -1;

        callback(&id, &c, ctx);

        if (!c.has_parent) break;

        id = c.parent;
    }

    return 0;
}

int head_read(ObjectID *id_out) {
    FILE *f = fopen(HEAD_FILE, "r");
    if (!f) return -1;

    char line[512];

    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return -1;
    }

    fclose(f);

    line[strcspn(line, "\r\n")] = '\0';

    if (strncmp(line, "ref: ", 5) == 0) {
        char ref_path[512];

        snprintf(ref_path, sizeof(ref_path), "%s/%s", PES_DIR, line + 5);

        f = fopen(ref_path, "r");
        if (!f) return -1;

        if (!fgets(line, sizeof(line), f)) {
            fclose(f);
            return -1;
        }

        fclose(f);

        line[strcspn(line, "\r\n")] = '\0';

        if (strlen(line) == 0) return -1;
    }

    return hex_to_hash(line, id_out);
}

int head_update(const ObjectID *new_commit) {
    FILE *f = fopen(HEAD_FILE, "r");
    if (!f) return -1;

    char line[512];

    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return -1;
    }

    fclose(f);

    line[strcspn(line, "\r\n")] = '\0';

    char target_path[520];

    if (strncmp(line, "ref: ", 5) == 0) {
        snprintf(target_path, sizeof(target_path), "%s/%s", PES_DIR, line + 5);

        FILE *check = fopen(target_path, "a");
        if (!check) return -1;
        fclose(check);
    } else {
        snprintf(target_path, sizeof(target_path), "%s", HEAD_FILE);
    }

    f = fopen(target_path, "w");
    if (!f) return -1;

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(new_commit, hex);

    fprintf(f, "%s\n", hex);

    fflush(f);
    fsync(fileno(f));
    fclose(f);

    return 0;
}

// ─── TODO IMPLEMENTED ───────────────────────────────────────────────────────

int commit_create(const char *message, ObjectID *commit_id_out) {
    Index index;

    if (index_load(&index) != 0) {

        return -1;
    }

    ObjectID tree_id;

    if (tree_from_index(&tree_id) != 0) {

        return -1;
    }

    Commit commit;
    memset(&commit, 0, sizeof(commit));

    commit.tree = tree_id;

    // Parent commit
    if (head_read(&commit.parent) == 0) {
        commit.has_parent = 1;
    } else {
        commit.has_parent = 0;
    }

    // Hardcoded author for stability
    snprintf(commit.author, sizeof(commit.author), "PES User");

    // Timestamp
    commit.timestamp = (uint64_t)time(NULL);

    // Message
    snprintf(commit.message, sizeof(commit.message), "%s", message);

    // Serialize
    void *raw_data;
    size_t raw_len;

    if (commit_serialize(&commit, &raw_data, &raw_len) != 0) {

        return -1;
    }

    // Store commit object
    if (object_write(OBJ_COMMIT, raw_data, raw_len, commit_id_out) != 0) {

        free(raw_data);
        return -1;
    }

    free(raw_data);

    // Update HEAD
    if (head_update(commit_id_out) != 0) {

        return -1;
    }

    return 0;
}
