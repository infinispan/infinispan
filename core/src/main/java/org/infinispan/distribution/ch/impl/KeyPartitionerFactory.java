package org.infinispan.distribution.ch.impl;

import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.distribution.group.impl.GroupingPartitioner;
import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;

/**
 * Key partitioner factory that uses the hash function defined in the configuration.
 *
 * In the future, we will probably remove the hash function from the configuration and leave only the
 * key partitioner.
 *
 * @author Dan Berindei
 * @since 8.2
 */
@DefaultFactoryFor(classes = KeyPartitioner.class)
public class KeyPartitionerFactory extends AbstractNamedCacheComponentFactory
      implements AutoInstantiableFactory {
   @Inject private GroupManager groupManager;

   private KeyPartitioner getConfiguredPartitioner() {
      HashConfiguration hashConfiguration = configuration.clustering().hash();
      KeyPartitioner partitioner = hashConfiguration.keyPartitioner();
      partitioner.init(hashConfiguration);
      return partitioner;
   }

   @Override
   public Object construct(String componentName) {

      if (configuration.clustering().cacheMode().needsStateTransfer()) {
         KeyPartitioner partitioner = getConfiguredPartitioner();
         if (groupManager == null)
            return partitioner;

         // Grouping is enabled. Since the configured partitioner will not be registered in the component
         // registry, we need to inject dependencies explicitly.
         componentRegistry.wireDependencies(partitioner);
         GroupingPartitioner groupingPartitioner = new GroupingPartitioner(partitioner, groupManager);
         return groupingPartitioner;
      } else if (configuration.persistence().stores().stream().filter(StoreConfiguration::segmented).findFirst().isPresent()) {
         // If store is segmented we still have to find consistent hashes
         return getConfiguredPartitioner();
      } else {
         return SingleSegmentKeyPartitioner.getInstance();
      }
   }
}
