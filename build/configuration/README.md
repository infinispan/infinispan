# Build Configuration sub-project

This is a centralized place to configure how Infinispan (and other dependenct components, e.g. infinispan-spark)
is going to be built, and most importantly, a single place for dependency management configuration.

This is achieved by creating a comprehensive BOM to control dependency versions,
while avoiding specifying them explicitly in `<dependencies>` sections.

Both BOM and the parent POMs of all Infinispan components inherit 
from a super-parent `infinispan-build-configuration-parent`, which, in a central location, 
defines various properties that affect the build processes.
