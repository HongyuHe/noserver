#! /bin/bash

# set -e
# set -x

DIR=$1

for file in $(ls $DIR)
do
    mv "$DIR/$file" $DIR/$(sed 's/.\{4\}$//' <<< "$file")_r-0.csv
done

