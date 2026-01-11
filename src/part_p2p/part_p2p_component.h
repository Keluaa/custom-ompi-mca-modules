#ifndef OMPI_CART_PLUGIN_PART_P2P_COMPONENT_H
#define OMPI_CART_PLUGIN_PART_P2P_COMPONENT_H

#include "ompi_config.h"
#include "ompi/mca/part/base/base.h"

BEGIN_C_DECLS

struct mca_part_p2p_component_t {
    mca_part_base_component_4_0_0_t super;
    int                             priority;
};
typedef struct mca_part_p2p_component_t mca_part_p2p_component_t;

OMPI_DECLSPEC extern mca_part_p2p_component_t mca_part_p2p_component;

END_C_DECLS

#endif //OMPI_CART_PLUGIN_PART_P2P_COMPONENT_H