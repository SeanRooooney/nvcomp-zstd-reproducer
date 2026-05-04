cmake_minimum_required(VERSION 3.23)
project(nvcomp_zstd_reproducer LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(CUDAToolkit REQUIRED)

# Try to find cudf and rmm via CMake config
# If not found, user must set paths manually
find_package(cudf QUIET)
find_package(rmm QUIET)

if(NOT cudf_FOUND)
  message(STATUS "cudf not found via CMake, using manual paths")
  # Set these paths according to your installation
  set(CUDF_INCLUDE_DIR "/usr/local/include" CACHE PATH "cudf include directory")
  set(CUDF_LIBRARY "/usr/local/lib/libcudf.so" CACHE FILEPATH "cudf library")
endif()

if(NOT rmm_FOUND)
  message(STATUS "rmm not found via CMake, using manual paths")
  set(RMM_INCLUDE_DIR "/usr/local/include" CACHE PATH "rmm include directory")
  set(RMM_LIBRARY "" CACHE FILEPATH "rmm library (optional, leave empty if header-only)")
endif()

# Create executables
add_executable(reproducer reproducer.cpp)
add_executable(reproducer_velox_pattern reproducer_velox_pattern.cpp)

# Link libraries for both executables
foreach(target reproducer reproducer_velox_pattern)
  if(cudf_FOUND)
    target_link_libraries(${target} PRIVATE cudf::cudf)
  else()
    target_include_directories(${target} PRIVATE ${CUDF_INCLUDE_DIR})
    target_link_libraries(${target} PRIVATE ${CUDF_LIBRARY})
  endif()

  if(rmm_FOUND)
    target_link_libraries(${target} PRIVATE rmm::rmm)
  else()
    target_include_directories(${target} PRIVATE ${RMM_INCLUDE_DIR})
    if(RMM_LIBRARY)
      target_link_libraries(${target} PRIVATE ${RMM_LIBRARY})
    endif()
  endif()

  target_link_libraries(${target} PRIVATE CUDA::cudart)

  # Set CUDA architectures (adjust as needed for your GPU)
  set_target_properties(${target} PROPERTIES
    CUDA_ARCHITECTURES "70;75;80;86;89;90"
  )
endforeach()
