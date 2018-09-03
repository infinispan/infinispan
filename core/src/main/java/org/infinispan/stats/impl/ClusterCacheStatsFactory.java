package org.infinispan.stats.impl;

import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.stats.ClusterCacheStats;

/**
 * ClusterCacheStatsFactory is a default factory class for {@link ClusterCacheStats}.
 * <p>
 * This is an internal class, not intended to be used by clients.
 * @author Vladimir Blagojevic
 * @since 7.1
 */
@DefaultFactoryFor(classes={ClusterCacheStats.class})
public class ClusterCacheStatsFactory extends AbstractNamedCacheComponentFactory implements
         AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   @Override
   public Object construct(String componentName) {
      return new ClusterCacheStatsImpl();
   }
}
