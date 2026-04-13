# Lab: Building PES-VCS — A Version Control System from Scratch

**Objective:** Build a local version control system that tracks file changes, stores snapshots efficiently, and supports branching. Every component maps directly to filesystem concepts.

**Platform:** Ubuntu 22.04

**Prerequisites:**

```bash
sudo apt update && sudo apt install -y gcc build-essential libssl-dev
```

---

## Understanding Git: What You're Building

Before writing code, you need to understand how Git actually works under the hood. Git is not magic — it's a content-addressable filesystem with a few clever data structures on top. Everything in this lab is based on Git's real design.

### The Big Picture

When you run `git commit`, Git doesn't store "changes" or "diffs." It stores **complete snapshots** of your entire project. This sounds wasteful, but Git uses two tricks to make it efficient:

1. **Content-addressable storage:** Every file is stored by the SHA hash of its contents. Same content = same hash = stored only once.
2. **Tree structures:** Directories are stored as "tree" objects that point to file contents, so unchanged files are just pointers to existing data.

```
Your project at commit A:          Your project at commit B:
                                   (only README changed)

    root/                              root/
    ├── README.md  ─────┐              ├── README.md  ─────┐
    ├── src/            │              ├── src/            │
    │   └── main.c ─────┼─┐            │   └── main.c ─────┼─┐
    └── Makefile ───────┼─┼─┐          └── Makefile ───────┼─┼─┐
                        │ │ │                              │ │ │
                        ▼ ▼ ▼                              ▼ ▼ ▼
    Object Store:       ┌─────────────────────────────────────────────┐
                        │  a1b2c3 (README v1)    ← only this is new   │
                        │  d4e5f6 (README v2)                         │
                        │  789abc (main.c)       ← shared by both!    │
                        │  fedcba (Makefile)     ← shared by both!    │
                        └─────────────────────────────────────────────┘
```

### The Three Object Types

Git stores exactly three types of objects (plus tags, which we skip). The entire version control system is built from these:

#### 1. Blob (Binary Large Object)

A blob is just file contents. No filename, no permissions — just the raw bytes.

```
blob 16\0Hello, World!\n
     ↑    ↑
     │    └── The actual file content
     └─────── Size in bytes
```

The blob is stored at a path determined by its SHA-256 hash. If two files have identical contents, they share one blob.

#### 2. Tree

A tree represents a directory. It's a list of entries, each pointing to a blob (file) or another tree (subdirectory).

```
tree 128\0
100644 blob a1b2c3d4... README.md
100755 blob e5f6a7b8... build.sh        ← executable file
040000 tree 9c0d1e2f... src             ← subdirectory
       ↑    ↑           ↑
       │    │           └── name
       │    └── hash of the object
       └─────── mode (permissions + type)
```

The mode field encodes both the file type and permissions:

- `100644` — regular file, not executable
- `100755` — regular file, executable
- `040000` — directory (tree)
- `120000` — symbolic link

#### 3. Commit

A commit ties everything together. It points to a tree (the project snapshot) and contains metadata.

```
tree 9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d
parent a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0    ← previous commit
author Alice <alice@example.com> 1699900000 +0000
committer Alice <alice@example.com> 1699900000 +0000

Add new feature
```

The parent pointer creates a linked list of history:

```
    C3 ──────► C2 ──────► C1 ──────► (no parent)
    │          │          │
    ▼          ▼          ▼
  Tree3      Tree2      Tree1
    │          │          │
    ▼          ▼          ▼
  Files      Files      Files
```

### How Objects Connect

Here's a complete picture of how one commit represents a project state:

```
                    ┌─────────────────────────────────┐
                    │           COMMIT                │
                    │  tree: 7a3f...                  │
                    │  parent: 4b2e...                │
                    │  author: Alice                  │
                    │  message: "Add feature"         │
                    └─────────────┬───────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────────┐
                    │         TREE (root)             │
                    │  100644 blob f1a2... README.md  │
                    │  040000 tree 8b3c... src        │
                    │  100644 blob 9d4e... Makefile   │
                    └──────┬──────────┬───────────────┘
                           │          │
              ┌────────────┘          └────────────┐
              ▼                                    ▼
┌─────────────────────────┐          ┌─────────────────────────┐
│      TREE (src)         │          │     BLOB (README.md)    │
│ 100644 blob a5f6 main.c │          │  # My Project           │
│ 100644 blob b6a7 util.c │          │  This is...             │
└───────────┬─────────────┘          └─────────────────────────┘
            │
    ┌───────┴───────┐
    ▼               ▼
┌────────┐     ┌────────┐
│ BLOB   │     │ BLOB   │
│main.c  │     │util.c  │
│contents│     │contents│
└────────┘     └────────┘
```

### References: Human-Readable Names

Nobody wants to type `git checkout a1b2c3d4e5f6...`. References are files that map names to commit hashes.

```
.git/
├── HEAD                    # "ref: refs/heads/main" or a commit hash
└── refs/
    └── heads/
        ├── main            # Contains: a1b2c3d4e5f6...
        └── feature         # Contains: 9f8e7d6c5b4a...
```

**HEAD** is special — it usually points to a branch (not directly to a commit):

```bash
$ cat .git/HEAD
ref: refs/heads/main

$ cat .git/refs/heads/main  
a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0
```

When you commit, Git:

1. Creates the new commit object (pointing to parent)
2. Updates the branch file to contain the new commit's hash
3. HEAD still points to the branch, so it "moves" automatically

```
Before commit:                    After commit:
                                  
HEAD ─► main ─► C2 ─► C1         HEAD ─► main ─► C3 ─► C2 ─► C1
                                                  │
                                              (new commit)
```

### The Index (Staging Area)

The index is Git's "preparation area" for the next commit. It's a binary file (`.git/index`) that records:

- Which files are staged
- Their blob hashes
- Metadata (timestamps, sizes) for fast change detection

```
Working Directory          Index               Repository (HEAD)
─────────────────         ─────────           ─────────────────
README.md (modified) ─┐
                      ├── git add ──► README.md (staged) 
src/main.c           │                src/main.c          ──► Last commit's
Makefile             │                Makefile                snapshot
                     │
                     └── (not staged yet, won't be in next commit)
```

The workflow:

1. `git add file.txt` → computes blob hash, stores blob, updates index
2. `git commit` → builds tree from index, creates commit, updates branch ref

### Content-Addressable Storage

The key insight: **objects are named by their content's hash**.

```python
# Pseudocode for storing an object
def store_object(content):
    hash = sha256(content)
    path = f".git/objects/{hash[0:2]}/{hash[2:]}"
    write_file(path, content)
    return hash
```

This gives us:

- **Deduplication:** Identical files stored once
- **Integrity:** Hash verifies data isn't corrupted
- **Immutability:** You can't modify an object (changing content = different hash = different object)

The objects directory is sharded by the first two characters of the hash to avoid huge directories:

```
.git/objects/
├── 2f/
│   └── 8a3b5c7d9e...     ← hash starts with "2f"
├── a1/
│   ├── 9c4e6f8a0b...     ← hash starts with "a1"  
│   └── b2d4f6a8c0...
└── ff/
    └── 1234567890...
```

### Exploring a Real Git Repository

You can inspect Git's internals yourself:

```bash
# Initialize a test repo
mkdir test-repo && cd test-repo
git init

# Create a file and commit
echo "Hello" > hello.txt
git add hello.txt
git commit -m "First commit"

# See the objects Git created
find .git/objects -type f

# Examine an object (Git compresses with zlib, so use git cat-file)
git cat-file -t <hash>     # Show type: blob, tree, or commit
git cat-file -p <hash>     # Show contents

# See what HEAD points to
cat .git/HEAD
cat .git/refs/heads/main

# See the index
git ls-files --stage
```

### How Common Operations Work

Now you can understand what Git commands actually do:

|Command|What It Actually Does|
|---|---|
|`git init`|Create `.git/` directory structure, write initial HEAD|
|`git add file`|Hash file contents → store blob → update index entry|
|`git status`|Compare working dir vs index vs HEAD tree|
|`git commit`|Build tree from index → create commit object → update branch ref|
|`git log`|Start at HEAD, follow parent pointers, print each commit|
|`git branch foo`|Create file `.git/refs/heads/foo` containing current commit hash|
|`git checkout foo`|Update HEAD to point to foo, sync working directory to foo's tree|
|`git diff`|Compare blob contents between index/HEAD/working dir|
|`git gc`|Find all objects reachable from refs, delete the rest|

### What You'll Build

In this lab, you'll implement a simplified Git called **PES-VCS** with these commands:

```
pes init        # Create .pes/ repository structure
pes add <file>  # Stage files (hash + update index)  
pes status      # Show modified/staged/untracked files
pes commit -m   # Create commit from staged files
pes log         # Walk and display commit history
pes branch      # Create/list branches
pes checkout    # Switch branches, update working directory
pes diff        # Show file differences
pes gc          # Garbage collect unreachable objects
```

Each phase of the lab builds one layer of this system, from low-level object storage up to high-level commands.

---

## What You'll Build

```
my_project/
├── .pes/
│   ├── objects/          # Content-addressable blob/tree/commit storage
│   │   ├── 2f/
│   │   │   └── 8a3b...   # Sharded by first 2 chars of hash
│   │   └── a1/
│   │       └── 9c4e...
│   ├── refs/
│   │   └── heads/
│   │       └── main      # Branch pointers (just files containing commit hash)
│   ├── index             # Staging area
│   ├── HEAD              # Current branch reference
│   └── config            # Repository configuration
└── (working directory files)
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WORKING DIRECTORY                              │
│                    (actual files you edit)                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                              pes add <file>
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 INDEX                                        │
│                    (staged changes, ready to commit)                         │
│                    Format: mode hash path                                    │
│                    100644 a1b2c3... src/main.c                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                             pes commit -m "msg"
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            OBJECT STORE                                      │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                                  │
│  │  BLOB   │    │  TREE   │    │ COMMIT  │                                  │
│  │ (file   │◄───│ (dir    │◄───│ (snap-  │                                  │
│  │ content)│    │ listing)│    │  shot)  │                                  │
│  └─────────┘    └─────────┘    └─────────┘                                  │
│                                     │                                        │
│  Stored at: .pes/objects/XX/YYY...  │                                        │
│  Filename = SHA256 hash of content  │                                        │
└─────────────────────────────────────┼───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 REFS                                         │
│         .pes/refs/heads/main  →  contains commit hash                        │
│         .pes/HEAD             →  "ref: refs/heads/main"                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Object Storage Foundation

**Filesystem Concepts:** Content-addressable storage, directory sharding, atomic writes, hashing for integrity

### 1.1 — The Object Store

Every piece of data (file contents, directory listings, commits) is stored as an "object" named by its SHA-256 hash. This gives us:

- **Deduplication:** Same content → same hash → stored once
- **Integrity:** Hash verifies content wasn't corrupted
- **Immutability:** Objects never change (changing content = different hash)

```c
// pes.h — Core data structures
#ifndef PES_H
#define PES_H

#include <stdint.h>
#include <stddef.h>

#define HASH_SIZE 32        // SHA-256 = 32 bytes
#define HASH_HEX_SIZE 64    // Hex string representation
#define PES_DIR ".pes"
#define OBJECTS_DIR ".pes/objects"
#define REFS_DIR ".pes/refs/heads"
#define INDEX_FILE ".pes/index"
#define HEAD_FILE ".pes/HEAD"

typedef enum {
    OBJ_BLOB,    // File content
    OBJ_TREE,    // Directory listing
    OBJ_COMMIT   // Snapshot with metadata
} ObjectType;

typedef struct {
    uint8_t hash[HASH_SIZE];
} ObjectID;

// Convert hash to hex string
void hash_to_hex(const ObjectID *id, char *hex_out);

// Convert hex string to hash
int hex_to_hash(const char *hex, ObjectID *id_out);

#endif
```

### 1.2 — Implement Object Writing

```c
// object.c — Implement these functions

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/sha.h>

// Compute SHA-256 hash of data
void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(id_out->hash, &ctx);
}

// Get the path where an object should be stored
// Format: .pes/objects/XX/YYYYYYYY... (first 2 hex chars as directory)
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

// Write an object to the store
// Returns 0 on success, -1 on error
//
// IMPORTANT: This must be ATOMIC. If we crash mid-write, the object
// store must not be corrupted. Strategy:
//   1. Write to a temp file in the same directory
//   2. fsync the temp file
//   3. rename temp file to final name (atomic on POSIX)
//   4. fsync the directory
//
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // TODO: Implement
    //
    // 1. Prepend type header: "blob <size>\0" or "tree <size>\0" or "commit <size>\0"
    // 2. Compute hash of header + data
    // 3. Create shard directory if needed (.pes/objects/XX/)
    // 4. Write to temp file
    // 5. fsync temp file
    // 6. Rename to final path
    // 7. fsync directory
    //
    // Return the object ID in id_out
    
    return -1;
}

// Read an object from the store
// Caller must free *data_out
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // TODO: Implement
    //
    // 1. Construct path from hash
    // 2. Read file
    // 3. Parse header to get type and size
    // 4. Verify hash matches content (integrity check!)
    // 5. Return type and data
    
    return -1;
}

// Check if an object exists
int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}
```

### 1.3 — Testing Object Storage

```c
// test_objects.c
#include "pes.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

void test_blob_storage(void) {
    const char *content = "Hello, PES-VCS!\n";
    ObjectID id;
    
    // Write blob
    int rc = object_write(OBJ_BLOB, content, strlen(content), &id);
    assert(rc == 0);
    
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    printf("Stored blob with hash: %s\n", hex);
    
    // Verify file exists in correct location
    char path[512];
    object_path(&id, path, sizeof(path));
    printf("Object stored at: %s\n", path);
    
    // Read it back
    ObjectType type;
    void *data;
    size_t len;
    rc = object_read(&id, &type, &data, &len);
    assert(rc == 0);
    assert(type == OBJ_BLOB);
    assert(len == strlen(content));
    assert(memcmp(data, content, len) == 0);
    free(data);
    
    printf("✓ Blob storage test passed\n");
}

void test_deduplication(void) {
    const char *content = "Duplicate content\n";
    ObjectID id1, id2;
    
    object_write(OBJ_BLOB, content, strlen(content), &id1);
    object_write(OBJ_BLOB, content, strlen(content), &id2);
    
    // Same content = same hash
    assert(memcmp(&id1, &id2, sizeof(ObjectID)) == 0);
    
    printf("✓ Deduplication test passed\n");
}

void test_integrity(void) {
    const char *content = "Test integrity\n";
    ObjectID id;
    object_write(OBJ_BLOB, content, strlen(content), &id);
    
    // Corrupt the stored file
    char path[512];
    object_path(&id, path, sizeof(path));
    
    FILE *f = fopen(path, "r+b");
    fseek(f, 20, SEEK_SET);  // Seek into content
    fputc('X', f);           // Corrupt one byte
    fclose(f);
    
    // Read should detect corruption
    ObjectType type;
    void *data;
    size_t len;
    int rc = object_read(&id, &type, &data, &len);
    assert(rc == -1);  // Should fail integrity check
    
    printf("✓ Integrity check test passed\n");
}

int main(void) {
    // Initialize repository
    system("rm -rf .pes");
    system("mkdir -p .pes/objects .pes/refs/heads");
    
    test_blob_storage();
    test_deduplication();
    test_integrity();
    
    return 0;
}
```

**📸 Screenshot 1A:** Output of test program showing all tests passing.

**📸 Screenshot 1B:** `find .pes/objects -type f` showing the sharded directory structure with stored objects.

### 1.4 — Analysis Questions

**Q1.1:** Why do we shard objects into subdirectories (`.pes/objects/ab/cdef...`) instead of storing all objects in one flat directory? What performance problem does this avoid?

**Q1.2:** The `object_write` function must be atomic. Explain what could go wrong if we wrote directly to the final path instead of using write-temp-then-rename. What filesystem guarantee makes `rename()` safe here?

**Q1.3:** Git uses SHA-1 (160 bits). We use SHA-256 (256 bits). Calculate the probability of a hash collision after storing 1 billion objects for each. Why is Git migrating to SHA-256?

---

## Phase 2: Trees and Blobs

**Filesystem Concepts:** Directory representation, recursive structures, file modes and permissions

### 2.1 — Understanding Trees

A **tree** object represents a directory. It lists entries, each containing:

- Mode (file type and permissions)
- Object hash (blob for file, tree for subdirectory)
- Name

```
Tree object format:
<mode> <name>\0<32-byte-hash><mode> <name>\0<32-byte-hash>...

Example (conceptual):
100644 README.md    → blob a1b2c3...
100755 build.sh     → blob d4e5f6...
040000 src          → tree 789abc...
```

### 2.2 — Implement Tree Handling

```c
// tree.h
#ifndef TREE_H
#define TREE_H

#include "pes.h"

#define MAX_TREE_ENTRIES 1024

typedef struct {
    uint32_t mode;          // File mode (100644 for regular, 100755 for executable, 040000 for directory)
    ObjectID hash;          // Hash of blob or subtree
    char name[256];         // Entry name
} TreeEntry;

typedef struct {
    TreeEntry entries[MAX_TREE_ENTRIES];
    int count;
} Tree;

// Parse a tree object from raw data
int tree_parse(const void *data, size_t len, Tree *tree_out);

// Serialize a tree to raw data for storage
// Caller must free *data_out
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out);

// Create a tree from the current index
int tree_from_index(Tree *tree_out);

// Recursively write a tree and all its subtrees to the object store
// Returns the root tree's object ID
int tree_write_recursive(const char *dir_path, ObjectID *id_out);

#endif
```

```c
// tree.c — Implement these functions
#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// Mode constants
#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000
#define MODE_SYMLINK   0120000

// Determine the mode for a file
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;
    
    if (S_ISDIR(st.st_mode)) return MODE_DIR;
    if (S_ISLNK(st.st_mode)) return MODE_SYMLINK;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

int tree_parse(const void *data, size_t len, Tree *tree_out) {
    // TODO: Parse binary tree format
    // Format: <mode-octal-ascii> <name>\0<32-byte-hash>
    //
    // Example bytes: "100644 hello.txt\0" + 32 bytes of hash
    //
    // Iterate through entries, populate tree_out->entries
    
    return -1;
}

int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    // TODO: Serialize tree to binary format
    // Sort entries by name (Git requirement for consistent hashing)
    // Write each entry in format: mode + space + name + null + hash
    
    return -1;
}

int tree_write_recursive(const char *dir_path, ObjectID *id_out) {
    // TODO: Recursively snapshot a directory
    //
    // 1. Open directory, iterate entries (skip . and .. and .pes)
    // 2. For each file: read content, write as blob, record hash
    // 3. For each subdirectory: recurse, record tree hash
    // 4. Build tree object from collected entries
    // 5. Write tree object
    // 6. Return tree's hash in id_out
    //
    // This is the core of "pes commit" — snapshot entire working directory
    
    return -1;
}
```

### 2.3 — Testing Trees

```c
// test_tree.c
void test_tree_snapshot(void) {
    // Create test directory structure
    system("mkdir -p test_project/src");
    system("echo 'int main() {}' > test_project/src/main.c");
    system("echo '# README' > test_project/README.md");
    system("echo 'build' > test_project/.pesignore");
    
    ObjectID root_tree;
    int rc = tree_write_recursive("test_project", &root_tree);
    assert(rc == 0);
    
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&root_tree, hex);
    printf("Root tree hash: %s\n", hex);
    
    // Read and print tree structure
    ObjectType type;
    void *data;
    size_t len;
    object_read(&root_tree, &type, &data, &len);
    
    Tree tree;
    tree_parse(data, len, &tree);
    
    printf("Tree contents:\n");
    for (int i = 0; i < tree.count; i++) {
        char entry_hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&tree.entries[i].hash, entry_hex);
        printf("  %06o %s %.12s...\n", 
               tree.entries[i].mode, 
               tree.entries[i].name,
               entry_hex);
    }
    
    free(data);
    system("rm -rf test_project");
}
```

**📸 Screenshot 2A:** Tree snapshot output showing directory structure captured with correct modes.

**📸 Screenshot 2B:** `xxd .pes/objects/XX/YYY... | head -20` showing raw tree object format.

### 2.4 — Analysis Questions

**Q2.1:** Tree entries must be sorted by name before serialization. Why? What would happen if two developers created the same directory structure but entries were stored in filesystem enumeration order?

**Q2.2:** The mode `100644` vs `100755` only differs in the executable bit. Why does the VCS track this? What problem would occur if it didn't?

**Q2.3:** Git doesn't track empty directories. Based on the tree structure we've implemented, explain why this is a fundamental limitation (not just a design choice).

---

## Phase 3: The Index (Staging Area)

**Filesystem Concepts:** Binary file formats, memory-mapped I/O, file locking

### 3.1 — Understanding the Index

The **index** is the staging area — files you've marked for the next commit. It's a binary file mapping paths to blob hashes.

```
Index file format (.pes/index):

Header:
  4 bytes: "PESI" (magic)
  4 bytes: version (1)
  4 bytes: entry count

Per entry:
  4 bytes:  mode
  32 bytes: blob hash
  2 bytes:  name length
  N bytes:  name (no null terminator)
  
Footer:
  32 bytes: SHA-256 of all above (integrity check)
```

### 3.2 — Implement Index Operations

```c
// index.h
#ifndef INDEX_H
#define INDEX_H

#include "pes.h"

#define INDEX_MAGIC "PESI"
#define INDEX_VERSION 1
#define MAX_INDEX_ENTRIES 10000

typedef struct {
    uint32_t mode;
    ObjectID hash;
    uint64_t mtime_sec;
    uint32_t mtime_nsec;
    uint32_t size;
    char path[512];
} IndexEntry;

typedef struct {
    IndexEntry entries[MAX_INDEX_ENTRIES];
    int count;
} Index;

// Load index from disk
int index_load(Index *index);

// Save index to disk (atomic write!)
int index_save(const Index *index);

// Add or update a file in the index
// Reads the file, writes blob, updates index entry
int index_add(Index *index, const char *path);

// Remove a file from the index
int index_remove(Index *index, const char *path);

// Find entry by path, returns NULL if not found
IndexEntry* index_find(Index *index, const char *path);

// Compare index to working directory
// Lists: modified files, new files, deleted files
int index_status(const Index *index);

#endif
```

```c
// index.c — Implement these functions
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

int index_load(Index *index) {
    // TODO: Load index from .pes/index
    //
    // 1. Open file, get size
    // 2. Read or mmap the content
    // 3. Verify magic and version
    // 4. Parse entries
    // 5. Verify footer checksum
    //
    // If file doesn't exist, initialize empty index
    
    return -1;
}

int index_save(const Index *index) {
    // TODO: Save index atomically
    //
    // 1. Serialize header
    // 2. Serialize all entries (sorted by path!)
    // 3. Compute checksum footer
    // 4. Write to temp file
    // 5. fsync temp file
    // 6. Rename to .pes/index
    // 7. fsync directory
    //
    // CRITICAL: Must be atomic. Index corruption = repository corruption.
    
    return -1;
}

int index_add(Index *index, const char *path) {
    // TODO: Stage a file
    //
    // 1. Read file content
    // 2. Write as blob to object store
    // 3. Get file metadata (mode, mtime, size)
    // 4. Update or add entry in index
    // 5. Save index
    
    return -1;
}

int index_status(const Index *index) {
    // TODO: Compare working directory to index
    //
    // For each indexed file:
    //   - If file missing: "deleted: <path>"
    //   - If mtime/size changed: check content hash
    //     - If hash different: "modified: <path>"
    //
    // For each file in working directory:
    //   - If not in index: "untracked: <path>"
    //
    // Optimization: Use mtime+size as fast "unchanged" check
    //              Only hash content if metadata changed
    
    return -1;
}
```

### 3.3 — Build the CLI Commands

```c
// pes.c — Main entry point
#include "pes.h"
#include "object.h"
#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <string.h>

void cmd_init(void) {
    // Create .pes directory structure
    mkdir(PES_DIR, 0755);
    mkdir(OBJECTS_DIR, 0755);
    mkdir(REFS_DIR, 0755);
    
    // Create initial HEAD
    FILE *f = fopen(HEAD_FILE, "w");
    fprintf(f, "ref: refs/heads/main\n");
    fclose(f);
    
    printf("Initialized empty PES repository in .pes/\n");
}

void cmd_add(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: pes add <file>...\n");
        return;
    }
    
    Index index;
    index_load(&index);
    
    for (int i = 2; i < argc; i++) {
        if (index_add(&index, argv[i]) == 0) {
            printf("Added: %s\n", argv[i]);
        } else {
            fprintf(stderr, "Failed to add: %s\n", argv[i]);
        }
    }
}

void cmd_status(void) {
    Index index;
    index_load(&index);
    index_status(&index);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: pes <command> [args]\n");
        fprintf(stderr, "Commands: init, add, status, commit, log, diff, branch, checkout\n");
        return 1;
    }
    
    const char *cmd = argv[1];
    
    if (strcmp(cmd, "init") == 0) cmd_init();
    else if (strcmp(cmd, "add") == 0) cmd_add(argc, argv);
    else if (strcmp(cmd, "status") == 0) cmd_status();
    // ... more commands
    else fprintf(stderr, "Unknown command: %s\n", cmd);
    
    return 0;
}
```

**📸 Screenshot 3A:** Run `./pes init`, `./pes add file1.txt file2.txt`, `./pes status` — show output.

**📸 Screenshot 3B:** `xxd .pes/index | head -30` showing binary index format with your entries.

### 3.4 — Analysis Questions

**Q3.1:** The index stores `mtime` and `size` for each entry. How does this optimize `pes status`? What's the worst-case scenario where this optimization fails?

**Q3.2:** Why must index entries be sorted by path before saving? (Hint: Same reason as tree entries.)

**Q3.3:** We use `mmap` to read the index. What advantage does this provide over `read()`? What precaution must we take before modifying the mapped memory?

---

## Phase 4: Commits and History

**Filesystem Concepts:** Linked structures on disk, reference files, atomic pointer updates

### 4.1 — Understanding Commits

A **commit** object contains:

- Tree hash (root directory snapshot)
- Parent hash(es) (previous commit, or none for initial)
- Author info
- Timestamp
- Message

```
Commit object format:

tree <tree-hash-hex>
parent <parent-hash-hex>       (omitted for first commit)
author <name> <timestamp>
committer <name> <timestamp>

<commit message>
```

### 4.2 — Implement Commits

```c
// commit.h
#ifndef COMMIT_H
#define COMMIT_H

#include "pes.h"

typedef struct {
    ObjectID tree;
    ObjectID parent;
    int has_parent;
    char author[256];
    uint64_t timestamp;
    char message[4096];
} Commit;

// Create a commit from current index
// 1. Build tree from index
// 2. Get current HEAD as parent
// 3. Create commit object
// 4. Update HEAD ref
int commit_create(const char *message, ObjectID *commit_id_out);

// Parse a commit object
int commit_parse(const void *data, size_t len, Commit *commit_out);

// Serialize a commit for storage
int commit_serialize(const Commit *commit, void **data_out, size_t *len_out);

// Walk commit history from HEAD
// Calls callback for each commit, newest first
typedef void (*commit_walk_fn)(const ObjectID *id, const Commit *commit, void *ctx);
int commit_walk(commit_walk_fn callback, void *ctx);

#endif
```

```c
// commit.c
#include "commit.h"
#include "index.h"
#include "tree.h"
#include <time.h>

// Read the current HEAD commit hash
// Returns 0 if HEAD exists and points to a commit
// Returns -1 if repository is empty (no commits yet)
int head_read(ObjectID *id_out) {
    // TODO:
    // 1. Read .pes/HEAD
    // 2. If it starts with "ref: ", follow the reference
    //    - Read .pes/refs/heads/<branch>
    // 3. Parse hex hash into id_out
    
    return -1;
}

// Update HEAD (or current branch) to point to new commit
int head_update(const ObjectID *new_commit) {
    // TODO:
    // 1. Read .pes/HEAD to find current branch
    // 2. Write new commit hash to .pes/refs/heads/<branch>
    //
    // CRITICAL: This must be atomic!
    // Use write-temp-rename pattern
    // This is the "pointer swing" that makes commits appear
    
    return -1;
}

int commit_create(const char *message, ObjectID *commit_id_out) {
    // TODO:
    // 1. Load index
    // 2. Build tree from index (tree_from_index)
    // 3. Write tree to object store
    // 4. Get current HEAD as parent (may not exist)
    // 5. Build commit object
    // 6. Write commit to object store
    // 7. Update HEAD to point to new commit
    //
    // After this, the commit is "published" — visible to pes log
    
    return -1;
}

int commit_walk(commit_walk_fn callback, void *ctx) {
    // TODO:
    // 1. Read HEAD
    // 2. Load commit object
    // 3. Call callback
    // 4. If commit has parent, follow parent and repeat
    // 5. Stop at root commit (no parent)
    
    return -1;
}
```

### 4.3 — Implement `pes log`

```c
void log_callback(const ObjectID *id, const Commit *commit, void *ctx) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    
    time_t t = commit->timestamp;
    char timebuf[64];
    strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", localtime(&t));
    
    printf("\033[33mcommit %s\033[0m\n", hex);
    printf("Author: %s\n", commit->author);
    printf("Date:   %s\n", timebuf);
    printf("\n    %s\n\n", commit->message);
}

void cmd_log(void) {
    commit_walk(log_callback, NULL);
}
```

### 4.4 — Testing Commits

```bash
# Test sequence
./pes init
echo "Hello" > hello.txt
./pes add hello.txt
./pes commit -m "Initial commit"

echo "World" >> hello.txt
./pes add hello.txt
./pes commit -m "Add world"

echo "Goodbye" > bye.txt
./pes add bye.txt
./pes commit -m "Add farewell"

./pes log
```

**📸 Screenshot 4A:** Output of `./pes log` showing three commits with hashes, authors, timestamps, and messages.

**📸 Screenshot 4B:** `find .pes -type f | sort` showing object store growth after three commits.

**📸 Screenshot 4C:** `cat .pes/refs/heads/main` and `cat .pes/HEAD` showing reference chain.

### 4.5 — Analysis Questions

**Q4.1:** The reference file `.pes/refs/heads/main` contains just a 64-character hex hash. Why is updating this file the "commit point" — the moment when a commit becomes part of history?

**Q4.2:** If power fails after writing the commit object but before updating the ref, what state is the repository in? Is the commit recoverable? Why is this safe?

**Q4.3:** We update refs using write-temp-rename. What would happen if we used `write()` to overwrite in place and crashed after writing 32 of the 64 hex characters?

---

## Phase 5: Branching and Checkout

**Filesystem Concepts:** Symbolic references, working directory synchronization, conflict detection

### 5.1 — Understanding Branches

A **branch** is just a file in `.pes/refs/heads/` containing a commit hash. That's it. Creating a branch = creating a file. Switching branches = updating HEAD and syncing working directory.

### 5.2 — Implement Branching

```c
// branch.h
#ifndef BRANCH_H
#define BRANCH_H

#include "pes.h"

// Create a new branch pointing to current HEAD
int branch_create(const char *name);

// List all branches, mark current one
int branch_list(void);

// Delete a branch (refuse if it's current)
int branch_delete(const char *name);

// Switch to a different branch
// 1. Check for uncommitted changes (refuse if dirty)
// 2. Update HEAD to point to new branch
// 3. Update working directory to match branch's tree
int checkout(const char *branch_or_commit);

// Get current branch name (NULL if detached HEAD)
const char* current_branch(void);

#endif
```

```c
// checkout.c — The complex part
#include "branch.h"
#include "tree.h"
#include "commit.h"
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>

// Restore working directory to match a tree
int checkout_tree(const ObjectID *tree_id, const char *prefix) {
    // TODO:
    // 1. Read tree object
    // 2. For each entry:
    //    - If blob: write file content to working directory
    //    - If tree: mkdir and recurse
    // 3. Handle file modes (especially executable bit)
    //
    // CAREFUL: This overwrites working directory files!
    // Real implementation would be smarter about unchanged files.
    
    return -1;
}

// Remove files that exist in current tree but not in target tree
int remove_departed_files(const ObjectID *old_tree, const ObjectID *new_tree) {
    // TODO: Find files in old but not in new, delete them
    return -1;
}

int checkout(const char *target) {
    // TODO:
    // 1. Check if target is a branch name or commit hash
    // 2. Resolve to commit hash
    // 3. Load that commit's tree
    // 4. Compare to current working directory
    //    - Refuse if uncommitted changes would be overwritten
    // 5. Update working directory (remove old files, write new files)
    // 6. Update HEAD
    //    - If branch: HEAD = "ref: refs/heads/<name>"
    //    - If commit: HEAD = "<hash>" (detached HEAD)
    
    return -1;
}
```

### 5.3 — Testing Branches

```bash
./pes init
echo "main content" > file.txt
./pes add file.txt
./pes commit -m "Main commit"

./pes branch feature
./pes checkout feature

echo "feature content" >> file.txt
./pes add file.txt  
./pes commit -m "Feature work"

./pes checkout main
cat file.txt   # Should show only "main content"

./pes checkout feature
cat file.txt   # Should show both lines
```

**📸 Screenshot 5A:** Sequence showing branch create, checkout, divergent commits.

**📸 Screenshot 5B:** `./pes branch` output showing both branches, current marked with asterisk.

**📸 Screenshot 5C:** Contents of `file.txt` after checking out main vs feature.

### 5.4 — Analysis Questions

**Q5.1:** `./pes checkout main` needs to overwrite `file.txt`. If the user has uncommitted changes to `file.txt`, what should happen? Implement detection of this "dirty working directory" state.

**Q5.2:** Git's checkout is fast because it only touches changed files. How would you efficiently compute "files that differ between current tree and target tree" without reading every file from the object store?

**Q5.3:** "Detached HEAD" means HEAD contains a commit hash directly instead of a branch reference. What happens if you make commits in detached HEAD state? How would a user recover those commits?

---

---

## Submission Checklist

### Screenshots Required 

| Phase | Screenshot | What to Capture                                       |
| ----- | ---------- | ----------------------------------------------------- |
| 1     | 1A         | Test program output showing all tests passing         |
| 1     | 1B         | `find .pes/objects -type f` showing sharded structure |
| 2     | 2A         | Tree snapshot output with directory structure         |
| 2     | 2B         | `xxd` of raw tree object                              |
| 3     | 3A         | `pes init`, `pes add`, `pes status` sequence          |
| 3     | 3B         | `xxd .pes/index` showing binary format                |
| 4     | 4A         | `pes log` output with three commits                   |
| 4     | 4B         | `find .pes -type f` showing object growth             |
| 4     | 4C         | `cat` of refs/heads/main and HEAD                     |
| 5     | 5A         | Branch create and checkout sequence                   |
| 5     | 5B         | `pes branch` output with asterisk marker              |
| 5     | 5C         | File contents after switching branches                |


### Code Files Required (6 total)

| File         | Description                         |
| ------------ | ----------------------------------- |
| `pes.h`      | Core data structures and constants  |
| `object.c`   | Object store implementation         |
| `tree.c`     | Tree serialization and parsing      |
| `index.c`    | Staging area implementation         |
| `commit.c`   | Commit creation and history walking |
| `pes.c`      | CLI entry point                     |

### Analysis Questions 

|Phase|Questions|
|---|---|
|1|Q1.1, Q1.2, Q1.3|
|2|Q2.1, Q2.2, Q2.3|
|3|Q3.1, Q3.2, Q3.3|
|4|Q4.1, Q4.2, Q4.3|
|5|Q5.1, Q5.2, Q5.3|
|6|Q6.1, Q6.2|
|7|Q7.1, Q7.2, Q7.3|

---

## Final Integration Test

Your PES-VCS should handle this complete sequence:

```bash
# Initialize and first commit
./pes init
echo "version 1" > file.txt
./pes add file.txt
./pes commit -m "Initial commit"

# Create feature branch and diverge
./pes branch feature
./pes checkout feature
echo "feature work" >> file.txt
./pes add file.txt
./pes commit -m "Feature work"

# Switch back and verify isolation
./pes checkout main
cat file.txt              # Should show only "version 1"

# View history
./pes log
```

**📸 Final Screenshot:** Complete sequence working end-to-end.

---

## Filesystem Concept Coverage

|Concept|Where It Appears|
|---|---|
|Content-addressable storage|Phase 1 — object store|
|Directory sharding|Phase 1 — `.pes/objects/XX/`|
|Atomic writes|Phases 1, 3, 4 — all writes use temp+rename|
|Hashing for integrity|Phase 1 — corruption detection|
|Directory representation|Phase 2 — tree objects|
|File modes/permissions|Phase 2 — tree entries|
|Binary file formats|Phase 3 — index format|
|Memory-mapped I/O|Phase 3 — index loading|
|Reference files|Phase 4, 5 — branches and HEAD|
|Pointer swinging|Phase 4 — commit publication|
|Working directory sync|Phase 5 — checkout|
|Change detection|Phase 6 — diff and status|
|Reachability analysis|Phase 7 — garbage collection|

---

## Compilation

```bash
# Compile all files
gcc -Wall -Wextra -O2 -c object.c -o object.o
gcc -Wall -Wextra -O2 -c tree.c -o tree.o
gcc -Wall -Wextra -O2 -c index.c -o index.o
gcc -Wall -Wextra -O2 -c commit.c -o commit.o
gcc -Wall -Wextra -O2 -c branch.c -o branch.o
gcc -Wall -Wextra -O2 -c checkout.c -o checkout.o
gcc -Wall -Wextra -O2 -c gc.c -o gc.o
gcc -Wall -Wextra -O2 -c pes.c -o pes.o

# Link with OpenSSL for SHA-256
gcc -o pes pes.o object.o tree.o index.o commit.o branch.o checkout.o gc.o -lcrypto

# Or use a Makefile
make
```

---

## Makefile

```makefile
CC = gcc
CFLAGS = -Wall -Wextra -O2
LDFLAGS = -lcrypto

OBJS = object.o tree.o index.o commit.o branch.o checkout.o gc.o pes.o

pes: $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c pes.h
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f pes $(OBJS)
	rm -rf .pes

test: pes
	./test_sequence.sh

.PHONY: clean test
```

---

## Further Reading

To deepen your understanding of Git internals:

- **Git Internals chapter** from the Pro Git book: https://git-scm.com/book/en/v2/Git-Internals-Plumbing-and-Porcelain
- **Git from the inside out**: https://codewords.recurse.com/issues/two/git-from-the-inside-out
- **The Git Parable** (conceptual explanation): https://tom.preston-werner.com/2009/05/19/the-git-parable.html