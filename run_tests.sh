#!/bin/bash

skip_load_common=0
while getopts s o; do
  case $o in
  s) skip_load_common=1 ;;
  *) echo "Unknown argument"
  esac
done
shift "$((OPTIND - 1))"

if [ "$skip_load_common" == "0" ]; then
  echo "Loading common data";
  python load_common_data.py;
fi

echo "Running tests"
python -m unittest discover -v
