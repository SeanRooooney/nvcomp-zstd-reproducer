cmake_minimum_required(VERSION 3.23)
project(nvcomp_zstd_reproducer LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(CUDAToolkit REQUIRED)

# Try to find cudf and rmm via CMake config
find_package(cudf QUIET)
find_package(rmm QUIET)

if(NOT cudf_FOUND)
  message(STATUS "cudf not found via CMake, using manual paths")
  set(CUDF_INCLUDE_DIR "/usr/local/include" CACHE PATH "cudf include directory")
  set(CUDF_LIBRARY "/usr/local/lib/libcudf.so" CACHE FILEPATH "cudf library")
endif()

if(NOT rmm_FOUND)
  message(STATUS "rmm not found via CMake, using manual paths")
  set(RMM_INCLUDE_DIR "/usr/local/include" CACHE PATH "rmm include directory")
  set(RMM_LIBRARY "/usr/local/lib/librmm.so" CACHE FILEPATH "rmm library")
endif()

# Create executable
add_executable(reproducer reproducer.cpp)

if(cudf_FOUND)
  target_link_libraries(reproducer PRIVATE cudf::cudf)
else()
  target_include_directories(reproducer PRIVATE ${CUDF_INCLUDE_DIR})
  target_link_libraries(reproducer PRIVATE ${CUDF_LIBRARY})
endif()

if(rmm_FOUND)
  target_link_libraries(reproducer PRIVATE rmm::rmm)
else()
  target_include_directories(reproducer PRIVATE ${RMM_INCLUDE_DIR})
  target_link_libraries(reproducer PRIVATE ${RMM_LIBRARY})
endif()

target_link_libraries(reproducer PRIVATE CUDA::cudart)

set_target_properties(reproducer PROPERTIES
  CUDA_ARCHITECTURES "70;75;80;86;89;90"
)
