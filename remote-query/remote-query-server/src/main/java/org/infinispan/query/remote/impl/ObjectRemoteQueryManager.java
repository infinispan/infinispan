package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper;

/**
 * Implementation of {@link RemoteQueryManager} for caches storing deserialized content (Java Objects).
 *
 * @since 9.4
 */
class ObjectRemoteQueryManager extends BaseRemoteQueryManager {

   private final Map<String, ObjectRemoteQueryEngine> enginePerMediaType = new ConcurrentHashMap<>();

   protected final SerializationContext serCtx;

   private final SearchIntegrator searchIntegrator;
   private final ComponentRegistry cr;

   ObjectRemoteQueryManager(AdvancedCache<?, ?> cache, ComponentRegistry cr, QuerySerializers querySerializers) {
      super(cache, querySerializers);
      searchIntegrator = cr.getComponent(SearchIntegrator.class);
      this.cr = cr;
      this.serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cache.getCacheManager());
   }

   @Override
   public Class<? extends Matcher> getMatcherClass(MediaType mediaType) {
      return getQueryEngineForMediaType(mediaType).getMatcherClass();
   }

   @Override
   public BaseRemoteQueryEngine getQueryEngine(AdvancedCache<?, ?> cache) {
      return getQueryEngineForMediaType(cache.getValueDataConversion().getRequestMediaType());
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      return filterResult;
   }

   private ObjectRemoteQueryEngine getQueryEngineForMediaType(MediaType mediaType) {
      ObjectRemoteQueryEngine queryEngine = enginePerMediaType.get(mediaType.getTypeSubtype());
      if (queryEngine == null) {
         ReflectionMatcher matcher = mediaType.match(APPLICATION_PROTOSTREAM) ?
               getProtobufObjectReflectionMatcher() : getObjectReflectionMatcher();

         cr.registerComponent(matcher, matcher.getClass());
         queryEngine = new ObjectRemoteQueryEngine(cache, matcher.getClass(), searchIntegrator != null);
         enginePerMediaType.put(mediaType.getTypeSubtype(), queryEngine);
      }
      return queryEngine;
   }

   private ProtobufObjectReflectionMatcher getProtobufObjectReflectionMatcher() {
      ProtobufEntityNameResolver entityNameResolver = new ProtobufEntityNameResolver(serCtx);
      return searchIntegrator == null ? new ProtobufObjectReflectionMatcher(entityNameResolver, serCtx) :
            new ProtobufObjectReflectionMatcher(entityNameResolver, serCtx, searchIntegrator);
   }

   private ObjectReflectionMatcher getObjectReflectionMatcher() {
      EntityNameResolver reflectionEntityNamesResolver = new ReflectionEntityNamesResolver(cache.getClassLoader());
      if (searchIntegrator == null) {
         return new ObjectReflectionMatcher(reflectionEntityNamesResolver);
      }

      // If indexing is enabled, then use the known set of classes for lookup and the global classloader as a fallback.
      QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
      return new ObjectReflectionMatcher(new HibernateSearchPropertyHelper(searchIntegrator,
            name -> queryInterceptor.getKnownClasses().stream()
                  .filter(c -> c.getName().equals(name)).findFirst().orElse(reflectionEntityNamesResolver.resolve(name))));
   }
}
