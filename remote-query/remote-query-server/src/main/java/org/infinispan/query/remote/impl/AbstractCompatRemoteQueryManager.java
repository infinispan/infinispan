package org.infinispan.query.remote.impl;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.ProtobufMetadataManager;

/**
 * Base class for implementations of {@link RemoteQueryManager} for caches storing deserialized (compat mode) content.
 *
 * @since 9.2
 */
abstract class AbstractCompatRemoteQueryManager implements RemoteQueryManager {

   private final CompatibilityReflectionMatcher compatibilityReflectionMatcher;
   private final BaseRemoteQueryEngine queryEngine;

   private final Encoder keyEncoder;
   private final Encoder valueEncoder;

   protected final SerializationContext ctx;
   protected final boolean isIndexed;

   AbstractCompatRemoteQueryManager(ComponentRegistry cr) {
      SearchIntegrator searchIntegrator = cr.getComponent(SearchIntegrator.class);
      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      Configuration cfg = cr.getComponent(Configuration.class);
      isIndexed = cfg.indexing().index().isEnabled();
      ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) cr.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      this.ctx = protobufMetadataManager.getSerializationContext();

      EntityNameResolver entityNameResolver = createEntityNamesResolver(cr);

      this.compatibilityReflectionMatcher = createMatcher(entityNameResolver, ctx, searchIntegrator);
      cache.getComponentRegistry().registerComponent(compatibilityReflectionMatcher, CompatibilityReflectionMatcher.class);
      this.queryEngine = new CompatibilityQueryEngine(cache, isIndexed);
      this.keyEncoder = cache.getAdvancedCache().getKeyDataConversion().getEncoder();
      this.valueEncoder = cache.getAdvancedCache().getValueDataConversion().getEncoder();
   }

   abstract EntityNameResolver createEntityNamesResolver(ComponentRegistry cr);

   abstract CompatibilityReflectionMatcher createMatcher(EntityNameResolver entityNameResolver, SerializationContext ctx, SearchIntegrator searchIntegrator);

   @Override
   public Matcher getMatcher() {
      return compatibilityReflectionMatcher;
   }

   @Override
   public BaseRemoteQueryEngine getQueryEngine() {
      return queryEngine;
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      return filterResult;
   }


   @Override
   public Encoder getKeyEncoder() {
      return keyEncoder;
   }

   @Override
   public Encoder getValueEncoder() {
      return valueEncoder;
   }
}
