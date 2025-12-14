
## Building

- First download the sources for the target OpenMPI version.
  They are found here: https://www.open-mpi.org/software/ompi/v5.0/.
  _Do not clone the OpenMPI's Git repo! This will not work!_.
  Then untar somewhere, e.g. `tar -xzf ./openmpi-5.0.9.tar.gz`.
  The `OMPI_SRC_PATH` variable should be set to that directory.
- Configure: `cmake -B <build dir> -S . <config options>`. See below for the required options.
- Build: `cmake --build <build dir>`
- The resulting library can be found at `<build dir>/src/mca_car_test.so`

#### CMake configuration options

- `-DOMPI_SRC_PATH="<OpenMPI sources>"` is the source directory for the target OpenMPI version.
  - *Important*: components are independent of each other, therefore only the headers are required.
    You do not need to build OpenMPI.
    This allows you to build components even if you don't have access to the full sources of the target OpenMPI installation.
    However, some headers are generated from the configuration step, so you must run `./configure` with the right
    parameters. You can find them in the output of `ompi_info`. 
- `-DOMPI_COMPONENTS_PATH="<path>"` is the path where the components should be installed to.
  See below for how to find/create the right path.
- `-DPMIX_INCLUDE_PATH="<path>"` is the include path to PMIX.
  Only required if OpenMPI was configured with an external PMIX version. 
- `-DHWLOC_INCLUDE_PATH="<path>"` is the include path to Hwloc.
  Only required if OpenMPI was configured with an external Hwloc version.

## Compatibility

Tested OpenMPI versions: `v4`, `v5`.
Should work on later versions as well. 

## Installing a new MCA component

To load a new component, OpenMPI must be able to find it.
The MCA parameter `mca_base_component_path` is a list of folders where OpenMPI searches for components:
```shell
$ ompi_info --all --parsable | grep "mca_base_component_path"
...
mca:mca:base:param:mca_base_component_path:value:"/usr/local/lib/openmpi:/home/lb/.openmpi/components"
...
```
The `/usr/local/lib/openmpi` is OpenMPI's installation folder.
Don't touch that.
Instead, will shall use the home directory: `~/.openmpi/components` (or similar).
If it is present in `mca_base_component_path` (like above), you can skip to the installation step below.
If it isn't present, you must override its value.

#### Overriding `mca_base_component_path`

Override the OpenMPI configuration:
```shell
mkdir -p ~/.openmpi/custom_mca
echo "mca_base_component_path = <old value>:~/.openmpi/components" > ~/.openmpi/mca-params.conf
```
Replace `<old value>` by the old value of `mca_base_component_path`.
If OpenMPI read the new configuration, `mca_base_component_path` should be properly set now.

#### Checking if OpenMPI recognized the new components

Place the new MCA component at the right place for OpenMPI to find:
```shell
cp "<build dir>/src/mca_car_test.so" "~/.openmpi/custom_mca/mca_car_test.so"
```
