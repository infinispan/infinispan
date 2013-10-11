package org.infinispan.query.spi;

import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface ProgrammaticSearchMappingProvider {

   void defineMappings(Cache cache, SearchMapping searchMapping);
}
