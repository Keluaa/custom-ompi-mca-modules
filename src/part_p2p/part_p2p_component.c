
#include "part_p2p_component.h"
#include "part_p2p.h"
#include "part_p2p_request.h"

#include "ompi/communicator/communicator.h"


static int mca_part_p2p_component_register(void);
static int mca_part_p2p_component_open(void);
static int mca_part_p2p_component_close(void);
static mca_part_base_module_t* mca_part_p2p_component_initialize(int* priority, bool enable_progress_threads, bool enable_mpi_threads);
static int mca_part_p2p_component_finalize(void);


mca_part_p2p_component_t mca_part_p2p_component = {
    .super = {
        .partm_version = {
            MCA_PART_BASE_VERSION_2_0_0,

            .mca_component_name = "p2p",
            MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION, OMPI_RELEASE_VERSION),
            .mca_open_component = mca_part_p2p_component_open,
            .mca_close_component = mca_part_p2p_component_close,
            .mca_register_component_params = mca_part_p2p_component_register,
        },
        .partm_data = {
            MCA_BASE_METADATA_PARAM_NONE
        },
        .partm_init = mca_part_p2p_component_initialize,
        .partm_finalize = mca_part_p2p_component_finalize,
    },
    .priority = 5,
    .default_min_partition_size = 0,
};


static int mca_part_p2p_component_register(void)
{
    mca_base_component_t* version = &mca_part_p2p_component.super.partm_version;

    mca_base_component_var_register(version,
        "priority", "Priority of the P2P part component",
        MCA_BASE_VAR_TYPE_INT, NULL, 0,
        MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
        &mca_part_p2p_component.priority);

    mca_base_component_var_register(version,
        "default_min_partition_size",
        "User partitions may be aggregated to reduce the amount of requests done and increase their payload size, "
        "improving network usage. Partitions smaller than this threshold will be combined into a bigger one."
        "This threshold is in kilobytes. A value of '0' disables automatic aggregation.",
        MCA_BASE_VAR_TYPE_INT, NULL, 0,
        MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_5, MCA_BASE_VAR_SCOPE_LOCAL,
        &mca_part_p2p_component.default_min_partition_size);

    return OPAL_SUCCESS;
}


static int mca_part_p2p_component_open(void)
{
    OBJ_CONSTRUCT(&ompi_part_p2p_module.lock, opal_mutex_t);
    ompi_part_p2p_module.module_in_use = 0;
    ompi_part_p2p_module.next_tag = 0;

    OBJ_CONSTRUCT(&ompi_part_p2p_module.live_requests, opal_list_t);
    OBJ_CONSTRUCT(&ompi_part_p2p_module.requests, opal_free_list_t);
    opal_free_list_init(&ompi_part_p2p_module.requests,
                         sizeof(mca_part_p2p_request_t), opal_cache_line_size, OBJ_CLASS(mca_part_p2p_request_t),
                         0,opal_cache_line_size,
                         4, -1, 64,
                         NULL, 0, NULL, NULL, NULL);

    ompi_part_p2p_module.part_comm = MPI_COMM_NULL;
    ompi_part_p2p_module.part_comm_init = MPI_REQUEST_NULL;

    // TODO: init module

    /* For no good reason, the part base module has a hardcoded include list of other MCA modules.
     * This prevents part modules other than "persist" to be selected, unless we add our own module
     * name to this list.
     * See https://github.com/open-mpi/ompi/blob/80bd9de4121a0e11bf278c62a8f4df902eff5f70/ompi/mca/part/base/part_base_frame.c#L141
     */
    opal_pointer_array_add(&mca_part_base_part, strdup(mca_part_p2p_component.super.partm_version.mca_component_name));

    return OMPI_SUCCESS;
}


static int mca_part_p2p_component_close(void)
{
    if (MPI_REQUEST_NULL != ompi_part_p2p_module.part_comm_init) {
        ompi_request_free(&ompi_part_p2p_module.part_comm_init);
    }
    if (MPI_COMM_NULL != ompi_part_p2p_module.part_comm) {
        ompi_comm_free(&ompi_part_p2p_module.part_comm);
    }
    OBJ_DESTRUCT(&ompi_part_p2p_module.live_requests);
    OBJ_DESTRUCT(&ompi_part_p2p_module.requests);
    OBJ_DESTRUCT(&ompi_part_p2p_module.lock);
    return OMPI_SUCCESS;
}


static mca_part_base_module_t* mca_part_p2p_component_initialize(int* priority, bool enable_progress_threads, bool enable_mpi_threads)
{
    *priority = mca_part_p2p_component.priority;
    return &ompi_part_p2p_module.super;
}


static int mca_part_p2p_component_finalize(void)
{
    return OMPI_SUCCESS;
}
