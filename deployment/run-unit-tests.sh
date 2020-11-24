#!/bin/bash
#
# This assumes all of the OS-level configuration has been completed and git repo has already been cloned
#
# This script should be run from the repo's deployment directory
# cd deployment
# ./run-unit-tests.sh
#

[ "$DEBUG" == 'true' ] && set -x
set -e

# Get reference for all important folders
template_dir="$PWD"
source_dir="$template_dir/../source"
root_dir="$template_dir/.."

echo "------------------------------------------------------------------------------"
echo "[Init] Clean old folders"
echo "------------------------------------------------------------------------------"

cd $root_dir
if [ -d ".venv" ]; then
  rm -rf ".venv"
fi

echo "------------------------------------------------------------------------------"
echo "[Env] Create virtual environment and install dependencies"
echo "------------------------------------------------------------------------------"

virtualenv .venv
source .venv/bin/activate

cd $source_dir
pip install -r $source_dir/requirements-build-and-test.txt
cd -

echo "------------------------------------------------------------------------------"
echo "[Test] Run pytest with coverage"
echo "------------------------------------------------------------------------------"
cd $source_dir
pytest --cov --cov-report=term-missing

# deactivate the virtual environment
deactivate

cd $template_dir

