#!/bin/bash -e
destination="$1"
sourceDirs="$2"

# Function to copy jacoco execution files from DB matrix execution into main server/tests/target directory
# The exec files are renamed to have corresponding DB suffix for avoiding having duplicate names (i.e. replacing each other)
copy_files() {
    source_dir="$1"
    db="${source_dir##*-}"
    for file in "$source_dir"/*; do
        # Get the file name and extension
        filename=$(basename "$file")
        extension="${filename##*.}"
        filename_without_extension="${filename%.*}"

        #Forming new filename based on DB name
        filename="$filename_without_extension-$db.$extension"

        # Copy the file to the destination
        cp "$file" "$destination/server/tests/target/$filename"
    done
}

for i in $sourceDirs; do
    echo "Copying $i"
    db="${i##*-}"
    if [[ "$db" = "main" ]]; then
      cp -r  $i/test_dir/infinispan/. $destination
    else
      copy_files $i
    fi
done
echo "Merging complete!"