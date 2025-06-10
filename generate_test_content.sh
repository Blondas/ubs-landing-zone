#!/bin/bash

# Check if both destination folder and number of files are provided
if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <destination_folder> <number_of_files>"
  exit 1
fi

# Check if the second parameter is a positive integer
if ! [[ "$2" =~ ^[0-9]+$ ]] || [ "$2" -lt 1 ]; then
  echo "Error: Number of files must be a positive integer"
  exit 1
fi

DEST="$1"
NUM_FILES="$2"

mkdir -p "$DEST"
cd "$DEST" || exit

for ((i=1; i<=NUM_FILES; i++)); do
  dir="package_$i"
  mkdir "$dir"

  echo "id,name" > "$dir/sample_$i.csv"
  echo "1,Alice" >> "$dir/sample_$i.csv"

  echo "<root><item id='1'>Alice</item></root>" > "$dir/sample_$i.xml"

  touch "$dir/control.control"

  tar_name="archive_$i.tar"
  checksum_name="${tar_name%.tar}.md5"
  tar -cf "$tar_name" -C "$dir" .

  md5sum "$tar_name" | awk '{print $1}' > "$checksum_name"

  rm -r "$dir"

  echo "Created $tar_name and $checksum_name in $DEST"
done
