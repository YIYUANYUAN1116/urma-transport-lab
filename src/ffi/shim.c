#define _POSIX_C_SOURCE 200112L

#include "shim.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <urma_api.h>

struct urma_lab_runtime {
    urma_device_t *device;
    urma_context_t *context;
    uint32_t eid_index;
    uint32_t jfc_count;
    uint32_t segment_count;
    uint32_t jetty_count;
    uint32_t outstanding_wr_count;
};

struct urma_lab_jfc {
    urma_lab_runtime_t *runtime;
    urma_jfc_t *jfc;
};

struct urma_lab_segment {
    urma_lab_runtime_t *runtime;
    urma_target_seg_t *segment;
    void *memory;
    uint64_t length;
    uint32_t outstanding_wr_count;
};

struct urma_lab_jetty {
    urma_lab_runtime_t *runtime;
    urma_jetty_t *jetty;
    urma_target_jetty_t *target;
    int bound;
    uint32_t outstanding_wr_count;
};

struct urma_lab_wr {
    urma_lab_runtime_t *runtime;
    urma_lab_segment_t *segment;
    urma_lab_jetty_t *jetty;
    urma_sge_t sge;
    urma_jfs_wr_t send_wr;
    urma_jfr_wr_t recv_wr;
};

struct urma_lab_descriptor {
    urma_rjetty_t *rjetty;
    uint32_t length;
    urma_lab_jetty_descriptor_meta_t meta;
};

static int urma_lab_pointer_error(int fallback)
{
    return errno > 0 ? -errno : fallback;
}

int urma_lab_get_abi_baseline(urma_lab_abi_baseline_t *out)
{
    if (out == NULL) {
        return -EINVAL;
    }

    *out = (urma_lab_abi_baseline_t) {
        .shim_abi_version = URMA_LAB_SHIM_ABI_VERSION,
        .pointer_size = (uint32_t)sizeof(void *),
        .status_size = (uint32_t)sizeof(urma_status_t),
        .init_attr_size = (uint32_t)sizeof(urma_init_attr_t),
        .eid_size = (uint32_t)sizeof(urma_eid_t),
        .device_size = (uint32_t)sizeof(urma_device_t),
        .context_size = (uint32_t)sizeof(urma_context_t),
        .success_value = (int32_t)URMA_SUCCESS,
    };
    return 0;
}

int urma_lab_runtime_open(const char *device_name, uint32_t eid_index,
                          urma_lab_runtime_t **out)
{
    urma_lab_runtime_t *runtime;
    urma_status_t status;

    if (device_name == NULL || out == NULL) {
        return -EINVAL;
    }
    *out = NULL;

    status = urma_init(NULL);
    if (status != URMA_SUCCESS) {
        return (int)status;
    }

    runtime = calloc(1, sizeof(*runtime));
    if (runtime == NULL) {
        (void)urma_uninit();
        return -ENOMEM;
    }

    /* liburma currently declares this input as char *, but does not own it. */
    runtime->device = urma_get_device_by_name((char *)device_name);
    if (runtime->device == NULL) {
        free(runtime);
        (void)urma_uninit();
        return -ENODEV;
    }

    runtime->context = urma_create_context(runtime->device, eid_index);
    if (runtime->context == NULL) {
        free(runtime);
        (void)urma_uninit();
        return -EIO;
    }

    runtime->eid_index = eid_index;
    *out = runtime;
    return 0;
}

int urma_lab_runtime_query_device(urma_lab_runtime_t *runtime,
                                  urma_lab_device_capability_t *out)
{
    urma_device_attr_t attr = {0};
    urma_eid_info_t *eid_list;
    uint32_t eid_count = 0;
    uint32_t i;
    int selected_eid_found = 0;
    urma_status_t status;

    if (runtime == NULL || runtime->device == NULL || out == NULL) {
        return -EINVAL;
    }
    if (sizeof(urma_eid_t) > URMA_LAB_EID_STORAGE_BYTES ||
        sizeof(runtime->device->name) > URMA_LAB_DEVICE_NAME_BYTES) {
        return -EOVERFLOW;
    }

    status = urma_query_device(runtime->device, &attr);
    if (status != URMA_SUCCESS) {
        return (int)status;
    }

    errno = 0;
    eid_list = urma_get_eid_list(runtime->device, &eid_count);
    if (eid_list == NULL) {
        return urma_lab_pointer_error(-ENODEV);
    }
    if (eid_count > URMA_LAB_MAX_EIDS) {
        urma_free_eid_list(eid_list);
        return -EOVERFLOW;
    }

    (void)memset(out, 0, sizeof(*out));
    (void)memcpy(out->device_name, runtime->device->name,
                 sizeof(runtime->device->name));
    out->device_name[URMA_LAB_DEVICE_NAME_BYTES - 1] = '\0';
    out->transport_type = (int32_t)runtime->device->type;
    out->selected_eid_index = runtime->eid_index;
    out->eid_count = eid_count;
    for (i = 0; i < eid_count; ++i) {
        out->eids[i].index = eid_list[i].eid_index;
        out->eids[i].length = (uint32_t)sizeof(eid_list[i].eid);
        (void)memcpy(out->eids[i].bytes, eid_list[i].eid.raw,
                     sizeof(eid_list[i].eid));
        if (eid_list[i].eid_index == runtime->eid_index) {
            selected_eid_found = 1;
        }
    }
    urma_free_eid_list(eid_list);

    if (selected_eid_found == 0) {
        return -ENOENT;
    }

    out->max_jfc = attr.dev_cap.max_jfc;
    out->max_jfs = attr.dev_cap.max_jfs;
    out->max_jfr = attr.dev_cap.max_jfr;
    out->max_jetty = attr.dev_cap.max_jetty;
    out->max_jfc_depth = attr.dev_cap.max_jfc_depth;
    out->max_jfs_depth = attr.dev_cap.max_jfs_depth;
    out->max_jfr_depth = attr.dev_cap.max_jfr_depth;
    out->max_jfs_inline_len = attr.dev_cap.max_jfs_inline_len;
    out->max_jfs_sge = attr.dev_cap.max_jfs_sge;
    out->max_jfs_rsge = attr.dev_cap.max_jfs_rsge;
    out->max_jfr_sge = attr.dev_cap.max_jfr_sge;
    out->max_msg_size = attr.dev_cap.max_msg_size;
    out->transport_modes = attr.dev_cap.trans_mode;
    out->page_size_cap = attr.dev_cap.page_size_cap;
    return 0;
}

int urma_lab_jfc_create(urma_lab_runtime_t *runtime, uint32_t depth,
                        urma_lab_jfc_t **out)
{
    urma_jfc_cfg_t cfg = {0};
    urma_lab_jfc_t *jfc;

    if (runtime == NULL || runtime->context == NULL || out == NULL || depth == 0) {
        return -EINVAL;
    }
    *out = NULL;

    jfc = calloc(1, sizeof(*jfc));
    if (jfc == NULL) {
        return -ENOMEM;
    }
    cfg.depth = depth;
    /* TODO(M1-verify): confirm NULL JFCE polling support on the target provider. */
    errno = 0;
    jfc->jfc = urma_create_jfc(runtime->context, &cfg);
    if (jfc->jfc == NULL) {
        int error = urma_lab_pointer_error(-EIO);
        free(jfc);
        return error;
    }

    jfc->runtime = runtime;
    runtime->jfc_count++;
    *out = jfc;
    return 0;
}

int urma_lab_jfc_delete(urma_lab_jfc_t *jfc)
{
    urma_status_t status;
    urma_lab_runtime_t *runtime;

    if (jfc == NULL || jfc->jfc == NULL || jfc->runtime == NULL) {
        return -EINVAL;
    }
    runtime = jfc->runtime;
    if (runtime->outstanding_wr_count != 0) {
        return -EBUSY;
    }
    status = urma_delete_jfc(jfc->jfc);
    if (status != URMA_SUCCESS) {
        return (int)status;
    }
    if (runtime->jfc_count > 0) {
        runtime->jfc_count--;
    }
    jfc->jfc = NULL;
    free(jfc);
    return 0;
}

int urma_lab_segment_create(urma_lab_runtime_t *runtime, uint64_t length,
                            uint64_t alignment, urma_lab_segment_t **out)
{
    urma_seg_cfg_t cfg = {0};
    urma_lab_segment_t *segment;
    int alloc_status;

    if (runtime == NULL || runtime->context == NULL || out == NULL || length == 0 ||
        length > SIZE_MAX || alignment < sizeof(void *) || alignment > SIZE_MAX ||
        (alignment & (alignment - 1)) != 0) {
        return -EINVAL;
    }
    *out = NULL;

    segment = calloc(1, sizeof(*segment));
    if (segment == NULL) {
        return -ENOMEM;
    }
    alloc_status = posix_memalign(&segment->memory, (size_t)alignment, (size_t)length);
    if (alloc_status != 0) {
        free(segment);
        return -alloc_status;
    }
    (void)memset(segment->memory, 0, (size_t)length);

    /*
     * TODO(M1-verify): confirm page/alignment and pinning behavior on the
     * target provider. M1 intentionally requests only local access.
     */
    cfg.va = (uint64_t)(uintptr_t)segment->memory;
    cfg.len = length;
    cfg.flag.value = 0;
    cfg.flag.bs.token_policy = URMA_TOKEN_NONE;
    cfg.flag.bs.cacheable = URMA_NON_CACHEABLE;
    cfg.flag.bs.access = URMA_ACCESS_LOCAL_ONLY;
    cfg.flag.bs.token_id_valid = URMA_TOKEN_ID_INVALID;
    errno = 0;
    segment->segment = urma_register_seg(runtime->context, &cfg);
    if (segment->segment == NULL) {
        int error = urma_lab_pointer_error(-EIO);
        free(segment->memory);
        free(segment);
        return error;
    }

    segment->runtime = runtime;
    segment->length = length;
    runtime->segment_count++;
    *out = segment;
    return 0;
}

int urma_lab_segment_delete(urma_lab_segment_t *segment)
{
    urma_status_t status;
    urma_lab_runtime_t *runtime;

    if (segment == NULL || segment->segment == NULL || segment->runtime == NULL) {
        return -EINVAL;
    }
    if (segment->outstanding_wr_count != 0) {
        return -EBUSY;
    }
    runtime = segment->runtime;
    status = urma_unregister_seg(segment->segment);
    if (status != URMA_SUCCESS) {
        /* Keep both registered handle and backing memory alive on failure. */
        return (int)status;
    }
    if (runtime->segment_count > 0) {
        runtime->segment_count--;
    }
    segment->segment = NULL;
    free(segment->memory);
    segment->memory = NULL;
    free(segment);
    return 0;
}

static int urma_lab_segment_range(const urma_lab_segment_t *segment,
                                  uint64_t offset, uint32_t length)
{
    if (segment == NULL || segment->segment == NULL || segment->memory == NULL ||
        length == 0 || offset > segment->length ||
        (uint64_t)length > segment->length - offset) {
        return -EINVAL;
    }
    return 0;
}

int urma_lab_segment_write(urma_lab_segment_t *segment, uint64_t offset,
                           const uint8_t *data, uint32_t length)
{
    if (data == NULL || urma_lab_segment_range(segment, offset, length) != 0) {
        return -EINVAL;
    }
    (void)memcpy((uint8_t *)segment->memory + offset, data, length);
    return 0;
}

int urma_lab_segment_read(const urma_lab_segment_t *segment, uint64_t offset,
                          uint8_t *out, uint32_t length)
{
    if (out == NULL || urma_lab_segment_range(segment, offset, length) != 0) {
        return -EINVAL;
    }
    (void)memcpy(out, (const uint8_t *)segment->memory + offset, length);
    return 0;
}

int urma_lab_jetty_create(urma_lab_runtime_t *runtime,
                          urma_lab_jfc_t *send_jfc,
                          urma_lab_jfc_t *recv_jfc,
                          const urma_lab_jetty_config_t *config,
                          urma_lab_jetty_t **out)
{
    urma_jfs_cfg_t jfs_cfg = {0};
    urma_jfr_cfg_t jfr_cfg = {0};
    urma_jetty_cfg_t jetty_cfg = {0};
    urma_lab_jetty_t *jetty;

    if (runtime == NULL || runtime->context == NULL || send_jfc == NULL ||
        recv_jfc == NULL || config == NULL || out == NULL ||
        send_jfc->runtime != runtime || recv_jfc->runtime != runtime ||
        send_jfc->jfc == NULL || recv_jfc->jfc == NULL ||
        config->send_depth == 0 || config->recv_depth == 0 ||
        config->max_send_sge == 0 || config->max_send_sge > UINT8_MAX ||
        config->max_recv_sge == 0 || config->max_recv_sge > UINT8_MAX) {
        return -EINVAL;
    }
    *out = NULL;

    jetty = calloc(1, sizeof(*jetty));
    if (jetty == NULL) {
        return -ENOMEM;
    }

    jfs_cfg.depth = config->send_depth;
    jfs_cfg.trans_mode = URMA_TM_RC;
    jfs_cfg.priority = URMA_MAX_PRIORITY;
    jfs_cfg.max_sge = (uint8_t)config->max_send_sge;
    jfs_cfg.max_rsge = 1;
    jfs_cfg.max_inline_data = 0;
    jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jfs_cfg.jfc = send_jfc->jfc;

    jfr_cfg.depth = config->recv_depth;
    jfr_cfg.flag.value = 0;
    jfr_cfg.flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
    jfr_cfg.trans_mode = URMA_TM_RC;
    jfr_cfg.max_sge = (uint8_t)config->max_recv_sge;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = recv_jfc->jfc;
    jfr_cfg.token_value.token = config->token;

    jetty_cfg.flag.value = 0;
    jetty_cfg.flag.bs.share_jfr = URMA_NO_SHARE_JFR;
    jetty_cfg.jfs_cfg = jfs_cfg;
    jetty_cfg.jfr_cfg = &jfr_cfg;

    errno = 0;
    jetty->jetty = urma_create_jetty(runtime->context, &jetty_cfg);
    if (jetty->jetty == NULL) {
        int error = urma_lab_pointer_error(-EIO);
        free(jetty);
        return error;
    }

    jetty->runtime = runtime;
    runtime->jetty_count++;
    *out = jetty;
    return 0;
}

int urma_lab_jetty_mark_error(urma_lab_jetty_t *jetty)
{
    urma_jetty_attr_t attr = {0};

    if (jetty == NULL || jetty->jetty == NULL) {
        return -EINVAL;
    }
    attr.mask = JETTY_STATE;
    attr.state = URMA_JETTY_STATE_ERROR;
    return (int)urma_modify_jetty(jetty->jetty, &attr);
}

int urma_lab_jetty_export_descriptor(urma_lab_jetty_t *jetty,
                                     urma_lab_descriptor_t **out)
{
    urma_lab_descriptor_t *descriptor;
    urma_status_t status;

    if (jetty == NULL || jetty->jetty == NULL || out == NULL) {
        return -EINVAL;
    }
    *out = NULL;
    descriptor = calloc(1, sizeof(*descriptor));
    if (descriptor == NULL) {
        return -ENOMEM;
    }

    /*
     * TODO(M2-verify): validate get_rjetty for non-shared JFR on the target
     * provider. The baseline API is used as designed; Rust never reads it.
     */
    status = urma_get_rjetty(jetty->jetty, &descriptor->rjetty,
                             &descriptor->length);
    if (status != URMA_SUCCESS) {
        free(descriptor);
        return (int)status;
    }
    if (descriptor->rjetty == NULL || descriptor->length < sizeof(urma_rjetty_t)) {
        urma_put_rjetty(descriptor->rjetty);
        free(descriptor);
        return -EPROTO;
    }

    descriptor->meta.transport_type = (uint32_t)jetty->runtime->device->type;
    descriptor->meta.eid_index = jetty->runtime->eid_index;
    descriptor->meta.jetty_id = descriptor->rjetty->jetty_id.id;
    descriptor->meta.opaque_len = descriptor->length;
    *out = descriptor;
    return 0;
}

int urma_lab_descriptor_get_meta(const urma_lab_descriptor_t *descriptor,
                                 urma_lab_jetty_descriptor_meta_t *out)
{
    if (descriptor == NULL || descriptor->rjetty == NULL || out == NULL) {
        return -EINVAL;
    }
    *out = descriptor->meta;
    return 0;
}

int urma_lab_descriptor_copy(const urma_lab_descriptor_t *descriptor,
                             uint8_t *out, uint32_t capacity)
{
    if (descriptor == NULL || descriptor->rjetty == NULL || out == NULL) {
        return -EINVAL;
    }
    if (capacity < descriptor->length) {
        return -ENOSPC;
    }
    (void)memcpy(out, descriptor->rjetty, descriptor->length);
    return 0;
}

void urma_lab_descriptor_free(urma_lab_descriptor_t *descriptor)
{
    if (descriptor == NULL) {
        return;
    }
    urma_put_rjetty(descriptor->rjetty);
    descriptor->rjetty = NULL;
    free(descriptor);
}

int urma_lab_jetty_import(urma_lab_jetty_t *jetty,
                          const urma_lab_jetty_descriptor_meta_t *meta,
                          const uint8_t *opaque_data, uint32_t opaque_len,
                          uint32_t token)
{
    urma_rjetty_t *rjetty;
    urma_token_t token_value = {0};

    if (jetty == NULL || jetty->runtime == NULL || jetty->jetty == NULL ||
        meta == NULL || opaque_data == NULL || opaque_len == 0 ||
        opaque_len != meta->opaque_len || opaque_len < sizeof(urma_rjetty_t) ||
        jetty->target != NULL ||
        meta->transport_type != (uint32_t)jetty->runtime->device->type) {
        return -EINVAL;
    }

    rjetty = malloc(opaque_len);
    if (rjetty == NULL) {
        return -ENOMEM;
    }
    (void)memcpy(rjetty, opaque_data, opaque_len);
    if (rjetty->jetty_id.id != meta->jetty_id ||
        rjetty->trans_mode != URMA_TM_RC || rjetty->type != URMA_JETTY) {
        free(rjetty);
        return -EPROTO;
    }

    token_value.token = token;
    errno = 0;
    jetty->target = urma_import_jetty(jetty->runtime->context, rjetty,
                                      &token_value);
    free(rjetty);
    if (jetty->target == NULL) {
        return urma_lab_pointer_error(-EIO);
    }
    return 0;
}

int urma_lab_jetty_bind(urma_lab_jetty_t *jetty)
{
    urma_status_t status;

    if (jetty == NULL || jetty->jetty == NULL || jetty->target == NULL) {
        return -EINVAL;
    }
    if (jetty->bound != 0) {
        return 0;
    }
    status = urma_bind_jetty(jetty->jetty, jetty->target);
    if (status != URMA_SUCCESS && status != URMA_EEXIST) {
        return (int)status;
    }
    jetty->bound = 1;
    return 0;
}

int urma_lab_jetty_unbind(urma_lab_jetty_t *jetty)
{
    urma_status_t status;

    if (jetty == NULL || jetty->jetty == NULL) {
        return -EINVAL;
    }
    if (jetty->outstanding_wr_count != 0) {
        return -EBUSY;
    }
    if (jetty->bound == 0) {
        return 0;
    }
    status = urma_unbind_jetty(jetty->jetty);
    if (status != URMA_SUCCESS) {
        return (int)status;
    }
    jetty->bound = 0;
    return 0;
}

int urma_lab_jetty_unimport(urma_lab_jetty_t *jetty)
{
    urma_status_t status;

    if (jetty == NULL || jetty->jetty == NULL) {
        return -EINVAL;
    }
    if (jetty->bound != 0) {
        return -EBUSY;
    }
    if (jetty->target == NULL) {
        return 0;
    }
    status = urma_unimport_jetty(jetty->target);
    if (status != URMA_SUCCESS) {
        return (int)status;
    }
    jetty->target = NULL;
    return 0;
}

int urma_lab_jetty_delete(urma_lab_jetty_t *jetty)
{
    urma_status_t status;
    urma_lab_runtime_t *runtime;

    if (jetty == NULL || jetty->runtime == NULL || jetty->jetty == NULL) {
        return -EINVAL;
    }
    if (jetty->bound != 0 || jetty->target != NULL ||
        jetty->outstanding_wr_count != 0) {
        return -EBUSY;
    }
    runtime = jetty->runtime;
    status = urma_delete_jetty(jetty->jetty);
    if (status != URMA_SUCCESS) {
        return (int)status;
    }
    if (runtime->jetty_count > 0) {
        runtime->jetty_count--;
    }
    jetty->jetty = NULL;
    free(jetty);
    return 0;
}

static int urma_lab_wr_create(urma_lab_jetty_t *jetty,
                              urma_lab_segment_t *segment, uint64_t offset,
                              uint32_t length, urma_lab_wr_t **out)
{
    urma_lab_wr_t *wr;

    if (jetty == NULL || jetty->runtime == NULL || jetty->jetty == NULL ||
        segment == NULL || segment->runtime != jetty->runtime || out == NULL ||
        urma_lab_segment_range(segment, offset, length) != 0) {
        return -EINVAL;
    }
    *out = NULL;
    wr = calloc(1, sizeof(*wr));
    if (wr == NULL) {
        return -ENOMEM;
    }
    wr->runtime = jetty->runtime;
    wr->segment = segment;
    wr->jetty = jetty;
    wr->sge.addr = (uint64_t)(uintptr_t)((uint8_t *)segment->memory + offset);
    wr->sge.len = length;
    wr->sge.tseg = segment->segment;
    wr->sge.user_tseg = NULL;
    *out = wr;
    return 0;
}

static void urma_lab_wr_posted(urma_lab_wr_t *wr)
{
    wr->runtime->outstanding_wr_count++;
    wr->segment->outstanding_wr_count++;
    wr->jetty->outstanding_wr_count++;
}

int urma_lab_post_send(urma_lab_jetty_t *jetty,
                       urma_lab_segment_t *segment, uint64_t offset,
                       uint32_t length, uint64_t user_ctx,
                       urma_lab_wr_t **out)
{
    urma_lab_wr_t *wr;
    urma_jfs_wr_t *bad_wr = NULL;
    urma_status_t status;
    int create_status;

    if (jetty == NULL || jetty->target == NULL || jetty->bound == 0) {
        return -ENOTCONN;
    }
    create_status = urma_lab_wr_create(jetty, segment, offset, length, &wr);
    if (create_status != 0) {
        return create_status;
    }
    wr->send_wr.opcode = URMA_OPC_SEND;
    wr->send_wr.flag.value = 0;
    wr->send_wr.flag.bs.complete_enable = 1;
    wr->send_wr.tjetty = jetty->target;
    wr->send_wr.user_ctx = user_ctx;
    wr->send_wr.send.src.sge = &wr->sge;
    wr->send_wr.send.src.num_sge = 1;
    wr->send_wr.send.imm_data = 0;
    wr->send_wr.next = NULL;
    status = urma_post_jetty_send_wr(jetty->jetty, &wr->send_wr, &bad_wr);
    if (status != URMA_SUCCESS) {
        free(wr);
        return (int)status;
    }
    urma_lab_wr_posted(wr);
    *out = wr;
    return 0;
}

int urma_lab_post_recv(urma_lab_jetty_t *jetty,
                       urma_lab_segment_t *segment, uint64_t offset,
                       uint32_t length, uint64_t user_ctx,
                       urma_lab_wr_t **out)
{
    urma_lab_wr_t *wr;
    urma_jfr_wr_t *bad_wr = NULL;
    urma_status_t status;
    int create_status = urma_lab_wr_create(jetty, segment, offset, length, &wr);

    if (create_status != 0) {
        return create_status;
    }
    wr->recv_wr.src.sge = &wr->sge;
    wr->recv_wr.src.num_sge = 1;
    wr->recv_wr.user_ctx = user_ctx;
    wr->recv_wr.next = NULL;
    status = urma_post_jetty_recv_wr(jetty->jetty, &wr->recv_wr, &bad_wr);
    if (status != URMA_SUCCESS) {
        free(wr);
        return (int)status;
    }
    urma_lab_wr_posted(wr);
    *out = wr;
    return 0;
}

void urma_lab_wr_complete(urma_lab_wr_t *wr)
{
    if (wr == NULL) {
        return;
    }
    if (wr->runtime->outstanding_wr_count > 0) {
        wr->runtime->outstanding_wr_count--;
    }
    if (wr->segment->outstanding_wr_count > 0) {
        wr->segment->outstanding_wr_count--;
    }
    if (wr->jetty->outstanding_wr_count > 0) {
        wr->jetty->outstanding_wr_count--;
    }
    free(wr);
}

int urma_lab_jfc_poll(urma_lab_jfc_t *jfc, uint32_t capacity,
                      urma_lab_completion_t *out)
{
    urma_cr_t cr[16] = {0};
    uint32_t i;
    int count;

    if (jfc == NULL || jfc->jfc == NULL || out == NULL ||
        capacity == 0 || capacity > 16) {
        return -EINVAL;
    }
    count = urma_poll_jfc(jfc->jfc, (int)capacity, cr);
    if (count <= 0) {
        return count;
    }
    for (i = 0; i < (uint32_t)count; ++i) {
        out[i].status = (int32_t)cr[i].status;
        out[i].opcode = (uint32_t)cr[i].opcode;
        out[i].user_ctx = cr[i].user_ctx;
        out[i].completion_len = cr[i].completion_len;
        out[i].is_recv = cr[i].flag.bs.s_r;
        out[i].is_jetty = cr[i].flag.bs.jetty;
        out[i].user_ctx_valid =
            (cr[i].status != URMA_CR_WR_SUSPEND_DONE &&
             cr[i].status != URMA_CR_WR_FLUSH_ERR_DONE);
    }
    return count;
}

int urma_lab_runtime_close(urma_lab_runtime_t *runtime)
{
    int first_error = 0;
    urma_status_t status;

    if (runtime == NULL) {
        return -EINVAL;
    }
    if (runtime->jetty_count != 0 || runtime->segment_count != 0 ||
        runtime->outstanding_wr_count != 0 ||
        runtime->jfc_count != 0) {
        return -EBUSY;
    }

    if (runtime->context != NULL) {
        status = urma_delete_context(runtime->context);
        if (status != URMA_SUCCESS) {
            /*
             * Do not unload provider code while a context may still refer to
             * ctx->ops. Keep the wrapper allocated so the failure is visible as
             * a deliberate process-lifetime leak instead of a dangling handle.
             */
            return (int)status;
        }
        runtime->context = NULL;
    }

    status = urma_uninit();
    if (status != URMA_SUCCESS) {
        first_error = (int)status;
    }

    free(runtime);
    return first_error;
}
