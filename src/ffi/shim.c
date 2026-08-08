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

int urma_lab_runtime_close(urma_lab_runtime_t *runtime)
{
    int first_error = 0;
    urma_status_t status;

    if (runtime == NULL) {
        return -EINVAL;
    }
    if (runtime->segment_count != 0 || runtime->jfc_count != 0) {
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
