package org.infinispan.query.remote.impl;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Norms;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapperFieldBridge;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.kohsuke.MetaInfServices;

/**
 * A ProgrammaticSearchMappingProvider that defines indexing for ProtobufValueWrapper with hibernate-search.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices
@SuppressWarnings("unused")
public final class ProgrammaticSearchMappingProviderImpl implements ProgrammaticSearchMappingProvider {

   private static final Log log = LogFactory.getLog(ProgrammaticSearchMappingProviderImpl.class, Log.class);

   private static final String INDEX_NAME_SUFFIX = "_protobuf";

   /**
    * Creates the name of the index given a cache name.
    */
   public static String getIndexName(String cacheName) {
      return cacheName + INDEX_NAME_SUFFIX;
   }

   @Override
   public void defineMappings(Cache cache, SearchMapping searchMapping) {
      if (log.isDebugEnabled()) {
         log.debugf("Enabling indexing for ProtobufValueWrapper in cache %s", cache.getName());
      }
      searchMapping.entity(ProtobufValueWrapper.class)
            .indexed()
            .indexName(getIndexName(cache.getName()))
            .interceptor(ProtobufValueWrapperIndexingInterceptor.class)
            .analyzerDiscriminator(ProtobufValueWrapperAnalyzerDiscriminator.class)
            .classBridgeInstance(new ProtobufValueWrapperFieldBridge(cache))
            .norms(Norms.NO)
            .analyze(Analyze.NO)
            .store(Store.NO);
   }
}
