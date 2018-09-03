package org.infinispan.factories;

import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.SimpleClusteredVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Version generator component factory. Version generators are used for situations where version or ids are needed, e.g.
 * data versioning, transaction recovery, or hotrod/memcached support.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 5.1
 */
@DefaultFactoryFor(classes = VersionGenerator.class, names = KnownComponentNames.TRANSACTION_VERSION_GENERATOR)
@Scope(Scopes.NAMED_CACHE)
public class VersionGeneratorFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public Object construct(String componentName) {
      if (KnownComponentNames.TRANSACTION_VERSION_GENERATOR.equals(componentName)) {
         return new NumericVersionGenerator();
      }
      if (Configurations.isTxVersioned(configuration)) {
         return configuration.clustering().cacheMode().isClustered() ?
                new SimpleClusteredVersionGenerator() :
                new NumericVersionGenerator();
      } else {
         return new NumericVersionGenerator();
      }
   }
}
