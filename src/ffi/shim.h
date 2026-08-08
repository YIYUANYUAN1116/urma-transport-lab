#ifndef URMA_LAB_SHIM_H
#define URMA_LAB_SHIM_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct urma_lab_runtime urma_lab_runtime_t;

#define URMA_LAB_SHIM_ABI_VERSION 1U

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

/*
 * Opens the smallest process-global chain: urma_init -> device -> context.
 * `device_name` must be NUL terminated and `out` must be a valid writable pointer.
 * On success, ownership of *out is transferred to the caller.
 */
int urma_lab_runtime_open(const char *device_name, uint32_t eid_index,
                          urma_lab_runtime_t **out);

/*
 * Destroys context before urma_uninit and frees the wrapper. The pointer must be
 * a unique live value returned by urma_lab_runtime_open.
 */
int urma_lab_runtime_close(urma_lab_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif
