#ifndef URMA_LAB_SHIM_H
#define URMA_LAB_SHIM_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct urma_lab_runtime urma_lab_runtime_t;
typedef struct urma_lab_jfc urma_lab_jfc_t;
typedef struct urma_lab_segment urma_lab_segment_t;

#define URMA_LAB_SHIM_ABI_VERSION 2U
#define URMA_LAB_DEVICE_NAME_BYTES 64U
#define URMA_LAB_EID_STORAGE_BYTES 32U
#define URMA_LAB_MAX_EIDS 256U

/* Stable, integer-only fingerprint of the UMDK headers used to build shim.c. */
typedef struct urma_lab_abi_baseline {
    uint32_t shim_abi_version;
    uint32_t pointer_size;
    uint32_t status_size;
    uint32_t init_attr_size;
    uint32_t eid_size;
    uint32_t device_size;
    uint32_t context_size;
    int32_t success_value;
} urma_lab_abi_baseline_t;

/* Copies the compile-time header fingerprint into `out`. */
int urma_lab_get_abi_baseline(urma_lab_abi_baseline_t *out);

typedef struct urma_lab_eid {
    uint32_t index;
    uint32_t length;
    uint8_t bytes[URMA_LAB_EID_STORAGE_BYTES];
} urma_lab_eid_t;

/* Rust-owned capability DTO. No pointer in this object belongs to liburma. */
typedef struct urma_lab_device_capability {
    char device_name[URMA_LAB_DEVICE_NAME_BYTES];
    int32_t transport_type;
    uint32_t selected_eid_index;
    uint32_t eid_count;
    urma_lab_eid_t eids[URMA_LAB_MAX_EIDS];
    uint32_t max_jfc;
    uint32_t max_jfs;
    uint32_t max_jfr;
    uint32_t max_jetty;
    uint32_t max_jfc_depth;
    uint32_t max_jfs_depth;
    uint32_t max_jfr_depth;
    uint32_t max_jfs_inline_len;
    uint32_t max_jfs_sge;
    uint32_t max_jfs_rsge;
    uint32_t max_jfr_sge;
    uint64_t max_msg_size;
    uint16_t transport_modes;
    uint16_t reserved;
    uint64_t page_size_cap;
} urma_lab_device_capability_t;

/*
 * Opens the smallest process-global chain: urma_init -> device -> context.
 * `device_name` must be NUL terminated and `out` must be a valid writable pointer.
 * On success, ownership of *out is transferred to the caller.
 */
int urma_lab_runtime_open(const char *device_name, uint32_t eid_index,
                          urma_lab_runtime_t **out);

/* Queries public device fields and copies them into a pointer-free DTO. */
int urma_lab_runtime_query_device(urma_lab_runtime_t *runtime,
                                  urma_lab_device_capability_t *out);

/* Creates a polling-mode JFC (`jfce == NULL`) with the requested depth. */
int urma_lab_jfc_create(urma_lab_runtime_t *runtime, uint32_t depth,
                        urma_lab_jfc_t **out);

int urma_lab_jfc_delete(urma_lab_jfc_t *jfc);

/* Allocates aligned zeroed memory, then registers it as local-only memory. */
int urma_lab_segment_create(urma_lab_runtime_t *runtime, uint64_t length,
                            uint64_t alignment, urma_lab_segment_t **out);

/* Unregisters the Segment before releasing its backing allocation. */
int urma_lab_segment_delete(urma_lab_segment_t *segment);

/*
 * Destroys context before urma_uninit and frees the wrapper. The pointer must be
 * a unique live value returned by urma_lab_runtime_open.
 */
int urma_lab_runtime_close(urma_lab_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif
