#!/bin/bash
#
# This assumes all of the OS-level configuration has been completed and git repo has already been cloned
#
# This script should be run from the repo's deployment directory
# cd deployment
# ./build-tests-dist.sh solution-name version-code
#
# Paramenters:
# - solution-name: name of the solution for consistency
#
# - version-code: version of the package

# always use debug
export DEBUG='true'
[ "$DEBUG" == 'true' ] && set -x
set -e

# Check to see if input has been provided:
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Please provide the trademark approved solution name and version."
    echo "For example: ./build-tests-dist.sh trademarked-solution-name v1.0.0"
    exit 1
fi

# Get reference for all important folders
depoloyment_dir="$PWD"
tests_dir="$depoloyment_dir/../tests"
tests_dist_dir="$depoloyment_dir/tests-s3-assets"

# Grabbing input parameters
solution_name="$1"
version="$2"

echo "------------------------------------------------------------------------------"
echo "[Init] Clean old distfolders"
echo "------------------------------------------------------------------------------"
rm -rf $tests_dist_dir
mkdir -p $tests_dist_dir

echo "------------------------------------------------------------------------------"
echo "[Packing] Tests and Test Schedule"
echo "------------------------------------------------------------------------------"

cd $tests_dir
zip -r9 ${solution_name}-tests.zip *
mv ${solution_name}-tests.zip ${tests_dist_dir}
