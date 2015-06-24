package org.infinispan.query.remote;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Norms;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.indexing.ProtobufValueWrapperFieldBridge;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices
public final class ProgrammaticSearchMappingProviderImpl implements ProgrammaticSearchMappingProvider {

   @Override
   public void defineMappings(Cache cache, SearchMapping searchMapping) {
      searchMapping.entity(ProtobufValueWrapper.class)
            .indexed()
            .classBridgeInstance(new ProtobufValueWrapperFieldBridge(cache))
            .norms(Norms.NO)
            .analyze(Analyze.NO)
            .store(Store.NO);
   }
}
