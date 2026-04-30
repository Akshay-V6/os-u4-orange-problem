// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;

    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }

    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();

    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);

    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];

    hash_to_hex(id, hex);

    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];

    object_path(id, path, sizeof(path));

    return access(path, F_OK) == 0;
}

// ─── TODO IMPLEMENTED ────────────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    char type_str[10];

    if (type == OBJ_BLOB) strcpy(type_str, "blob");
    else if (type == OBJ_TREE) strcpy(type_str, "tree");
    else if (type == OBJ_COMMIT) strcpy(type_str, "commit");
    else return -1;

    // Build header
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    // Build full object = header + data
    size_t full_len = header_len + len;
    unsigned char *full_data = malloc(full_len);
    if (!full_data) return -1;

    memcpy(full_data, header, header_len);
    memcpy(full_data + header_len, data, len);

    // Compute SHA-256 of full object
    compute_hash(full_data, full_len, id_out);

    // Deduplication
    if (object_exists(id_out)) {
        free(full_data);
        return 0;
    }

    // Final object path
    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));

    // Directory path
    char dir_path[512];
    strncpy(dir_path, final_path, sizeof(dir_path));
    char *slash = strrchr(dir_path, '/');

    if (!slash) {
        free(full_data);
        return -1;
    }

    *slash = '\0';

    // Ensure directories exist
    mkdir(".pes", 0755);
    mkdir(OBJECTS_DIR, 0755);
    mkdir(dir_path, 0755);

    // Temporary file
    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s/tempXXXXXX", dir_path);

    int fd = mkstemp(temp_path);
    if (fd < 0) {
        free(full_data);
        return -1;
    }

    // Write full object
    ssize_t written = write(fd, full_data, full_len);
    if (written != (ssize_t)full_len) {
        close(fd);
        unlink(temp_path);
        free(full_data);
        return -1;
    }

    // Flush file
    fsync(fd);
    close(fd);

    // Atomic rename
    if (rename(temp_path, final_path) != 0) {
        unlink(temp_path);
        free(full_data);
        return -1;
    }

    // Flush directory
    int dir_fd = open(dir_path, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    free(full_data);
    return 0;
}

// Read an object from the store.
//
// Returns 0 on success, -1 on error.
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];

    object_path(id, path, sizeof(path));

    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;

    // Read full file
    fseek(fp, 0, SEEK_END);
    size_t file_len = ftell(fp);
    rewind(fp);

    unsigned char *file_data = malloc(file_len);
    if (!file_data) {
        fclose(fp);
        return -1;
    }

    if (fread(file_data, 1, file_len, fp) != file_len) {
        free(file_data);
        fclose(fp);
        return -1;
    }

    fclose(fp);

    // Verify integrity
    ObjectID verify_id;
    compute_hash(file_data, file_len, &verify_id);

    if (memcmp(verify_id.hash, id->hash, HASH_SIZE) != 0) {
        free(file_data);
        return -1;
    }

    // Find header/data separator
    char *null_pos = memchr(file_data, '\0', file_len);
    if (!null_pos) {
        free(file_data);
        return -1;
    }

    size_t header_len = (null_pos - (char *)file_data) + 1;

    // Parse header
    char type_str[10];
    size_t size;

    if (sscanf((char *)file_data, "%9s %zu", type_str, &size) != 2) {
        free(file_data);
        return -1;
    }

    // Parse type
    if (strcmp(type_str, "blob") == 0) {
        *type_out = OBJ_BLOB;
    } else if (strcmp(type_str, "tree") == 0) {
        *type_out = OBJ_TREE;
    } else if (strcmp(type_str, "commit") == 0) {
        *type_out = OBJ_COMMIT;
    } else {
        free(file_data);
        return -1;
    }

    // Extract payload
    *data_out = malloc(size);
    if (!*data_out) {
        free(file_data);
        return -1;
    }

    memcpy(*data_out, file_data + header_len, size);
    *len_out = size;

    free(file_data);
    return 0;
}
