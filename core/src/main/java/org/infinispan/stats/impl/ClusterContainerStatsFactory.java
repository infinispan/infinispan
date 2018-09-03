package org.infinispan.stats.impl;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.stats.ClusterContainerStats;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@DefaultFactoryFor(classes = ClusterContainerStats.class)
public class ClusterContainerStatsFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   @SuppressWarnings("unchecked")
   @Override
   public Object construct(String componentName) {
      return new ClusterContainerStatsImpl();
   }
}
