package org.infinispan.distribution.ch.impl;

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

   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.clustering().cacheMode().needsStateTransfer()) {
         KeyPartitioner partitioner = configuration.clustering().hash().keyPartitioner();
         partitioner.init(configuration.clustering().hash());
         if (groupManager == null)
            return componentType.cast(partitioner);

         // Grouping is enabled. Since the configured partitioner will not be registered in the component
         // registry, we need to inject dependencies explicitly.
         componentRegistry.wireDependencies(partitioner);
         GroupingPartitioner groupingPartitioner = new GroupingPartitioner(partitioner, groupManager);
         return componentType.cast(groupingPartitioner);
      } else {
         return componentType.cast(SingleSegmentKeyPartitioner.getInstance());
      }
   }
}
