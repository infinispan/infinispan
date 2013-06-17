package org.infinispan.container.versioning;

/**
 * Generates versions
 *
 * @author Manik Surtani
 * @since 5.1
 */
public interface VersionGenerator {
   /**
    * Generates a new entry version
    * @return a new entry version
    */
   IncrementableEntryVersion generateNew();

   IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion);

   IncrementableEntryVersion nonExistingVersion();
}
