package org.infinispan.conflict;

import org.infinispan.configuration.cache.PartitionHandlingConfiguration;

public interface EntryMergePolicyFactory {
   <T> T createInstance(PartitionHandlingConfiguration config);
}
