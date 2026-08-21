#ifndef URMA_LAB_SHIM_H
#define URMA_LAB_SHIM_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct urma_lab_runtime urma_lab_runtime_t;
typedef struct urma_lab_jfce urma_lab_jfce_t;
typedef struct urma_lab_jfc urma_lab_jfc_t;
typedef struct urma_lab_segment urma_lab_segment_t;
typedef struct urma_lab_jetty urma_lab_jetty_t;
typedef struct urma_lab_descriptor urma_lab_descriptor_t;
typedef struct urma_lab_wr urma_lab_wr_t;

#define URMA_LAB_SHIM_ABI_VERSION 8U
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

typedef struct urma_lab_jetty_config {
    uint32_t send_depth;
    uint32_t recv_depth;
    uint32_t max_send_sge;
    uint32_t max_recv_sge;
    uint32_t token;
} urma_lab_jetty_config_t;

typedef struct urma_lab_jetty_descriptor_meta {
    uint32_t transport_type;
    uint32_t eid_index;
    uint32_t jetty_id;
    uint32_t opaque_len;
} urma_lab_jetty_descriptor_meta_t;

/* Pointer-free completion DTO copied from urma_cr_t by the C shim. */
typedef struct urma_lab_completion {
    int32_t status;
    uint32_t opcode;
    uint64_t user_ctx;
    uint32_t completion_len;
    uint8_t is_recv;
    uint8_t is_jetty;
    uint8_t user_ctx_valid;
    uint8_t reserved;
} urma_lab_completion_t;

/* Pointer-free input used to build a linked WR list inside the C shim. */
typedef struct urma_lab_wr_desc {
    uint64_t offset;
    uint64_t user_ctx;
    uint32_t length;
    uint8_t complete_enable;
    uint8_t reserved[3];
} urma_lab_wr_desc_t;

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

/* Creates one event channel shared by the runtime's send and receive JFCs. */
int urma_lab_jfce_create(urma_lab_runtime_t *runtime, urma_lab_jfce_t **out);
int urma_lab_jfce_delete(urma_lab_jfce_t *jfce);

/* Creates a JFC associated with `jfce` for hybrid polling/event notification. */
int urma_lab_jfc_create(urma_lab_runtime_t *runtime, urma_lab_jfce_t *jfce,
                        uint32_t depth,
                        urma_lab_jfc_t **out);

int urma_lab_jfc_delete(urma_lab_jfc_t *jfc);
int urma_lab_jfc_rearm(urma_lab_jfc_t *jfc);

#define URMA_LAB_JFCE_SEND_READY 1U
#define URMA_LAB_JFCE_RECV_READY 2U

/*
 * Waits for either known JFC. Returns 1 when at least one event was received,
 * 0 on timeout, or a negative error. A successful wait must be followed by
 * `urma_lab_jfce_ack` after polling the reported JFCs.
 */
int urma_lab_jfce_wait(urma_lab_jfce_t *jfce,
                       urma_lab_jfc_t *send_jfc,
                       urma_lab_jfc_t *recv_jfc,
                       int32_t timeout_ms, uint32_t *ready_mask);
int urma_lab_jfce_ack(urma_lab_jfce_t *jfce);

/* Allocates aligned zeroed memory, then registers it as local-only memory. */
int urma_lab_segment_create(urma_lab_runtime_t *runtime, uint64_t length,
                            uint64_t alignment, urma_lab_segment_t **out);

/* Unregisters the Segment before releasing its backing allocation. */
int urma_lab_segment_delete(urma_lab_segment_t *segment);
int urma_lab_segment_write(urma_lab_segment_t *segment, uint64_t offset,
                           const uint8_t *data, uint32_t length);
int urma_lab_segment_read(const urma_lab_segment_t *segment, uint64_t offset,
                          uint8_t *out, uint32_t length);
int urma_lab_segment_get_const(const urma_lab_segment_t *segment,
                               uint64_t offset, uint32_t length,
                               const uint8_t **out);
int urma_lab_segment_get_mut(urma_lab_segment_t *segment,
                             uint64_t offset, uint32_t length,
                             uint8_t **out);

/* Creates one RC duplex Jetty backed by an owned shared JFR. */
int urma_lab_jetty_create(urma_lab_runtime_t *runtime,
                          urma_lab_jfc_t *send_jfc,
                          urma_lab_jfc_t *recv_jfc,
                          const urma_lab_jetty_config_t *config,
                          urma_lab_jetty_t **out);

/* Optional M2/M3 shutdown transition; no CQ drain is performed here. */
int urma_lab_jetty_mark_error(urma_lab_jetty_t *jetty);

int urma_lab_jetty_export_descriptor(urma_lab_jetty_t *jetty,
                                     urma_lab_descriptor_t **out);
int urma_lab_descriptor_get_meta(const urma_lab_descriptor_t *descriptor,
                                 urma_lab_jetty_descriptor_meta_t *out);
int urma_lab_descriptor_copy(const urma_lab_descriptor_t *descriptor,
                             uint8_t *out, uint32_t capacity);
void urma_lab_descriptor_free(urma_lab_descriptor_t *descriptor);

int urma_lab_jetty_import(urma_lab_jetty_t *jetty,
                          const urma_lab_jetty_descriptor_meta_t *meta,
                          const uint8_t *opaque_data, uint32_t opaque_len,
                          uint32_t token);
int urma_lab_jetty_bind(urma_lab_jetty_t *jetty);
int urma_lab_jetty_unbind(urma_lab_jetty_t *jetty);
int urma_lab_jetty_unimport(urma_lab_jetty_t *jetty);
int urma_lab_jetty_delete(urma_lab_jetty_t *jetty);

/*
 * These functions build bitfield/union-bearing UMDK WR/SGE objects in C.
 * The returned owner must remain alive until its CQE is consumed.
 */
int urma_lab_post_send(urma_lab_jetty_t *jetty,
                       urma_lab_segment_t *segment, uint64_t offset,
                       uint32_t length, uint64_t user_ctx,
                       uint8_t complete_enable,
                       urma_lab_wr_t **out);
int urma_lab_post_recv(urma_lab_jetty_t *jetty,
                       urma_lab_segment_t *segment, uint64_t offset,
                       uint32_t length, uint64_t user_ctx,
                       urma_lab_wr_t **out);
/*
 * Posts one linked list. `out_posted` is the successfully submitted prefix;
 * only those entries in `out_wrs` are provider-owned and require completion.
 * On providers that return an error without bad_wr, the complete list is
 * conservatively treated as possibly posted and must not be reused early.
 */
int urma_lab_post_send_batch(urma_lab_jetty_t *jetty,
                             urma_lab_segment_t *segment,
                             const urma_lab_wr_desc_t *descs,
                             uint32_t count, urma_lab_wr_t **out_wrs,
                             uint32_t *out_posted);
int urma_lab_post_recv_batch(urma_lab_jetty_t *jetty,
                             urma_lab_segment_t *segment,
                             const urma_lab_wr_desc_t *descs,
                             uint32_t count, urma_lab_wr_t **out_wrs,
                             uint32_t *out_posted);
void urma_lab_wr_complete(urma_lab_wr_t *wr);

/* Non-blocking poll. Returns a count in [0, capacity], or a negative error. */
int urma_lab_jfc_poll(urma_lab_jfc_t *jfc, uint32_t capacity,
                      urma_lab_completion_t *out);

/*
 * Destroys context before urma_uninit and frees the wrapper. The pointer must be
 * a unique live value returned by urma_lab_runtime_open.
 */
int urma_lab_runtime_close(urma_lab_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif
