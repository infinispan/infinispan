#!/bin/bash

grep -R "Test.*enabled[ ]*=[ ]*false" * | grep -v .svn | sed -e "s/:.*//"

