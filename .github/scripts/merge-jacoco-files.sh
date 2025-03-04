#!/bin/bash -e
destination="$1"
sourceDirs="$2"

# Function to copy files with renamed duplicates
copy_files() {
    source_dir="$1"
    for file in "$source_dir"/*; do
        if [[ -d "$file" ]]; then
    	      cp -r "$file" "$destination"
        else
            # Get the file name and extension
            filename=$(basename "$file")
            extension="${filename##*.}"
            filename_without_extension="${filename%.*}"

            # If file exists, rename it to avoid clobbering
            if [[ -e "$destination/$filename" ]]; then
                counter=1
                # Create a new unique filename with a counter if a file with the same name exists
                while [[ -e "$destination/$filename_without_extension-$counter.$extension" ]]; do
                    ((counter++))
                done
                filename="$filename_without_extension-$counter.$extension"
            fi

            # Copy the file to the destination
            cp "$file" "$destination/$filename"
        fi
    done
}

for i in $sourceDirs; do
    echo "Copying $i"
    copy_files $i"/test_dir/infinispan/jacoco/"
done
echo "Merging complete!"