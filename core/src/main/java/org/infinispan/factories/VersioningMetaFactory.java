package org.infinispan.factories;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.SimpleClusteredVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * Version generator component factory. Version generators are used for
 * situations where version or ids are needed, e.g. data versioning,
 * transaction recovery, or hotrod/memcached support.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 5.1
 */
@DefaultFactoryFor(classes = VersionGenerator.class)
@SuppressWarnings("unused")
public class VersioningMetaFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   @Override
   public <T> T construct(Class<T> componentType) {
      // TODO: Eventually, NumericVersionGenerator and SimpleClusteredVersionGenerator should be merged into one...
      switch (configuration.versioning().scheme()) {
         case SIMPLE: {
            if (configuration.clustering().cacheMode().isClustered())
               return (T) new SimpleClusteredVersionGenerator();

            return (T) new NumericVersionGenerator();
         }
         default:
            return (T) new NumericVersionGenerator();
      }
   }

}
