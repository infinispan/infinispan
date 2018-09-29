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

/**
 * Implementation of {@link RemoteQueryManager} for caches storing deserialized content (Java Objects).
 *
 * @since 9.4
 */
class ObjectRemoteQueryManager extends BaseRemoteQueryManager {

   private final Map<String, BaseRemoteQueryEngine> enginePerMediaType = new ConcurrentHashMap<>();

   protected final SerializationContext serCtx;
   protected final boolean isIndexed;

   private final ComponentRegistry cr;

   ObjectRemoteQueryManager(ComponentRegistry cr, QuerySerializers querySerializers) {
      super(cr, querySerializers);
      this.cr = cr;
      this.isIndexed = cache.getCacheConfiguration().indexing().index().isEnabled();
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

   private BaseRemoteQueryEngine getQueryEngineForMediaType(MediaType mediaType) {
      BaseRemoteQueryEngine queryEngine = enginePerMediaType.get(mediaType.getTypeSubtype());
      if (queryEngine != null) return queryEngine;

      SearchIntegrator searchIntegrator = cr.getComponent(SearchIntegrator.class);

      EntityNameResolver entityNameResolver = createEntityNamesResolver(mediaType);

      ReflectionMatcher matcher = mediaType.match(APPLICATION_PROTOSTREAM) ?
            ProtobufObjectReflectionMatcher.create(entityNameResolver, serCtx, searchIntegrator) :
            ObjectReflectionMatcher.create(entityNameResolver, searchIntegrator);

      cr.registerComponent(matcher, matcher.getClass());
      ObjectRemoteQueryEngine engine = new ObjectRemoteQueryEngine(cache, matcher.getClass(), isIndexed);
      enginePerMediaType.put(mediaType.getTypeSubtype(), engine);
      return engine;
   }

   private EntityNameResolver createEntityNamesResolver(MediaType mediaType) {
      if (mediaType.match(APPLICATION_PROTOSTREAM)) {
         return new ProtobufEntityNameResolver(serCtx);
      } else {
         ClassLoader classLoader = cr.getGlobalComponentRegistry().getComponent(ClassLoader.class);

         ReflectionEntityNamesResolver reflectionEntityNamesResolver = new ReflectionEntityNamesResolver(classLoader);
         if (isIndexed) {
            // If indexing is enabled, then use the known set of classes for lookup and the global classloader as a fallback.
            QueryInterceptor qi = cr.getComponent(QueryInterceptor.class);
            return name -> qi.getKnownClasses().stream()
                  .filter(c -> c.getName().equals(name))
                  .findFirst()
                  .orElse(reflectionEntityNamesResolver.resolve(name));
         }
         return reflectionEntityNamesResolver;
      }
   }
}
