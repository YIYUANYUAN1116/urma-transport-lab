#include "shim.h"

#include <errno.h>
#include <stdlib.h>

#include <urma_api.h>

struct urma_lab_runtime {
    urma_device_t *device;
    urma_context_t *context;
};

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

    *out = runtime;
    return 0;
}

int urma_lab_runtime_close(urma_lab_runtime_t *runtime)
{
    int first_error = 0;
    urma_status_t status;

    if (runtime == NULL) {
        return -EINVAL;
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
