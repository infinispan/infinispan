#!/bin/sh

find $(pwd) -name '*Test.java' | xargs -I{} perl -ne 'if (/ (?<!abstract )class (\w+Test)\W/) { $class_name = $1; if ($annotation_line && $annotation_line !~ /.*testName *= *".*${class_name}".*/) { print "{}: ${class_name}: ${annotation_line}\n" } } chomp($_); $annotation_line = $_ if ($_ =~ /.*(\@Test|testName).*/);' {}
