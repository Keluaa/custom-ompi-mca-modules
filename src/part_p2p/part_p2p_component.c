
#include "ompi_config.h"
#include "ompi/mca/part/base/base.h"

#include "part_p2p.h"


static int mca_part_p2p_component_register(void);
static int mca_part_p2p_component_open(void);
static int mca_part_p2p_component_close(void);
static mca_part_base_module_t* mca_part_p2p_component_initialize(int* priority, bool enable_progress_threads, bool enable_mpi_threads);
static int mca_part_p2p_component_finalize(void);


mca_part_base_component_4_0_0_t mca_part_p2p_component = {
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
};


static int mca_part_p2p_component_register(void)
{
    // TODO: register MCA parameters
    return OPAL_SUCCESS;
}


static int mca_part_p2p_component_open(void)
{
    OBJ_CONSTRUCT(&ompi_part_p2p_module.lock, opal_mutex_t);
    ompi_part_p2p_module.module_in_use = 0;
    ompi_part_p2p_module.next_tag = 0;
    ompi_part_p2p_module.live_requests = OBJ_NEW(opal_list_t);
    ompi_part_p2p_module.part_comm = MPI_COMM_NULL;
    ompi_part_p2p_module.part_comm_init = MPI_REQUEST_NULL;

    // TODO: init module

    /* For no good reason, the part base module has a hardcoded include list of other MCA modules.
     * This prevents part modules other than "persist" to be selected, unless we add our own module
     * name to this list.
     * See https://github.com/open-mpi/ompi/blob/80bd9de4121a0e11bf278c62a8f4df902eff5f70/ompi/mca/part/base/part_base_frame.c#L141
     */
    opal_pointer_array_add(&mca_part_base_part, strdup(mca_part_p2p_component.partm_version.mca_component_name));

    return OMPI_SUCCESS;
}


static int mca_part_p2p_component_close(void)
{
    OBJ_DESTRUCT(&ompi_part_p2p_module.lock);
    return OMPI_SUCCESS;
}


static mca_part_base_module_t* mca_part_p2p_component_initialize(int* priority, bool enable_progress_threads, bool enable_mpi_threads)
{
    *priority = 5;
    return &ompi_part_p2p_module.super;
}


static int mca_part_p2p_component_finalize(void)
{
    return OMPI_SUCCESS;
}
