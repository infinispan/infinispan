package org.infinispan.stats.impl;

import static org.infinispan.stats.impl.LocalContainerStatsImpl.LOCAL_CONTAINER_STATS;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.stats.ClusterContainerStats;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@DefaultFactoryFor(classes = ClusterContainerStats.class, names = LOCAL_CONTAINER_STATS)
public class ClusterContainerStatsFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (componentName.equals(LOCAL_CONTAINER_STATS)) {
         return new LocalContainerStatsImpl();
      }
      return new ClusterContainerStatsImpl();
   }
}
