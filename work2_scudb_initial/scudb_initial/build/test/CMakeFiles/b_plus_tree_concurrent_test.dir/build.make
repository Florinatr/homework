# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.10

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/florina/scudb_initial/scudb_initial

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/florina/scudb_initial/scudb_initial/build

# Include any dependencies generated for this target.
include test/CMakeFiles/b_plus_tree_concurrent_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/b_plus_tree_concurrent_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/b_plus_tree_concurrent_test.dir/flags.make

test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o: test/CMakeFiles/b_plus_tree_concurrent_test.dir/flags.make
test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o: ../test/index/b_plus_tree_concurrent_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/florina/scudb_initial/scudb_initial/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o"
	cd /home/florina/scudb_initial/scudb_initial/build/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o -c /home/florina/scudb_initial/scudb_initial/test/index/b_plus_tree_concurrent_test.cpp

test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.i"
	cd /home/florina/scudb_initial/scudb_initial/build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/florina/scudb_initial/scudb_initial/test/index/b_plus_tree_concurrent_test.cpp > CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.i

test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.s"
	cd /home/florina/scudb_initial/scudb_initial/build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/florina/scudb_initial/scudb_initial/test/index/b_plus_tree_concurrent_test.cpp -o CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.s

test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o.requires:

.PHONY : test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o.requires

test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o.provides: test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o.requires
	$(MAKE) -f test/CMakeFiles/b_plus_tree_concurrent_test.dir/build.make test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o.provides.build
.PHONY : test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o.provides

test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o.provides.build: test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o


# Object files for target b_plus_tree_concurrent_test
b_plus_tree_concurrent_test_OBJECTS = \
"CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o"

# External object files for target b_plus_tree_concurrent_test
b_plus_tree_concurrent_test_EXTERNAL_OBJECTS =

test/b_plus_tree_concurrent_test: test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o
test/b_plus_tree_concurrent_test: test/CMakeFiles/b_plus_tree_concurrent_test.dir/build.make
test/b_plus_tree_concurrent_test: lib/libvtable.so
test/b_plus_tree_concurrent_test: lib/libsqlite3.so
test/b_plus_tree_concurrent_test: lib/libgtest.so
test/b_plus_tree_concurrent_test: test/CMakeFiles/b_plus_tree_concurrent_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/florina/scudb_initial/scudb_initial/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable b_plus_tree_concurrent_test"
	cd /home/florina/scudb_initial/scudb_initial/build/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/b_plus_tree_concurrent_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/b_plus_tree_concurrent_test.dir/build: test/b_plus_tree_concurrent_test

.PHONY : test/CMakeFiles/b_plus_tree_concurrent_test.dir/build

test/CMakeFiles/b_plus_tree_concurrent_test.dir/requires: test/CMakeFiles/b_plus_tree_concurrent_test.dir/index/b_plus_tree_concurrent_test.cpp.o.requires

.PHONY : test/CMakeFiles/b_plus_tree_concurrent_test.dir/requires

test/CMakeFiles/b_plus_tree_concurrent_test.dir/clean:
	cd /home/florina/scudb_initial/scudb_initial/build/test && $(CMAKE_COMMAND) -P CMakeFiles/b_plus_tree_concurrent_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/b_plus_tree_concurrent_test.dir/clean

test/CMakeFiles/b_plus_tree_concurrent_test.dir/depend:
	cd /home/florina/scudb_initial/scudb_initial/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/florina/scudb_initial/scudb_initial /home/florina/scudb_initial/scudb_initial/test /home/florina/scudb_initial/scudb_initial/build /home/florina/scudb_initial/scudb_initial/build/test /home/florina/scudb_initial/scudb_initial/build/test/CMakeFiles/b_plus_tree_concurrent_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/b_plus_tree_concurrent_test.dir/depend

