package org.infinispan.container.versioning;

/**
 * @since 10.0
 */
public interface LongEntryVersion extends IncrementableEntryVersion {

   long getVersion();
}
