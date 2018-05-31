package org.infinispan.query.spi;

import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;

/**
 * An advanced SPI to be implemented by Infinispan modules that want to customize the {@link SearchMapping} object right
 * before the bootstrap of the {@link org.hibernate.search.spi.SearchIntegrator} belonging to an indexed {@link Cache}.
 * The {@link SearchMapping} object provided via the "hibernate.search.model_mapping" config property is used as a
 * starting point if it was present, otherwise an empty {@link SearchMapping} is used.
 * <p>
 * Please note that in case multiple providers are found in classpath the order of invocation is not predictable so
 * implementations must be careful not to step on each others toes.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface ProgrammaticSearchMappingProvider {

   /**
    * Supply some custom programmatic mappings to Hibernate Search.
    *
    * @param cache         the indexed cache instance
    * @param searchMapping the mapping object to customize
    */
   void defineMappings(Cache cache, SearchMapping searchMapping);
}
