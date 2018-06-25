package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

/**
 * Implementation of {@link RemoteQueryManager} for caches storing deserialized content.
 *
 * @since 9.4
 */
class ObjectRemoteQueryManager implements RemoteQueryManager {

   private Map<String, BaseRemoteQueryEngine> enginePerMediaType = new ConcurrentHashMap<>();

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   protected final SerializationContext ctx;
   protected final boolean isIndexed;

   private final ComponentRegistry cr;
   private final AdvancedCache<?, ?> cache;

   ObjectRemoteQueryManager(ComponentRegistry cr) {
      this.cr = cr;
      this.cache = cr.getComponent(Cache.class).getAdvancedCache();
      Configuration cfg = cache.getCacheConfiguration();
      this.isIndexed = cfg.indexing().index().isEnabled();
      ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) cr.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      this.ctx = protobufMetadataManager.getSerializationContext();
      this.keyDataConversion = cache.getAdvancedCache().getKeyDataConversion();
      this.valueDataConversion = cache.getAdvancedCache().getValueDataConversion();
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
   public QueryRequest decodeQueryRequest(byte[] queryRequest, MediaType requestType) {
      return (QueryRequest) valueDataConversion.convert(queryRequest, requestType, QUERY_REQUEST_TYPE);
   }

   @Override
   public byte[] encodeQueryResponse(QueryResponse queryResponse, MediaType destinationType) {
      MediaType destination = destinationType;
      if (destinationType.match(APPLICATION_PROTOSTREAM)) destination = PROTOSTREAM_UNWRAPPED;

      return (byte[]) valueDataConversion.convert(queryResponse, APPLICATION_OBJECT, destination);
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      return filterResult;
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   private BaseRemoteQueryEngine getQueryEngineForMediaType(MediaType mediaType) {
      BaseRemoteQueryEngine queryEngine = enginePerMediaType.get(mediaType.getTypeSubtype());
      if (queryEngine != null) return queryEngine;

      SearchIntegrator searchIntegrator = cr.getComponent(SearchIntegrator.class);

      EntityNameResolver entityNameResolver = createEntityNamesResolver(mediaType);

      ReflectionMatcher matcher = mediaType.match(APPLICATION_PROTOSTREAM) ?
            ProtobufObjectReflectionMatcher.create(entityNameResolver, ctx, searchIntegrator) :
            ObjectReflectionMatcher.create(entityNameResolver, searchIntegrator);

      cr.registerComponent(matcher, matcher.getClass());
      ObjectRemoteQueryEngine engine = new ObjectRemoteQueryEngine(cache, matcher.getClass(), isIndexed);
      enginePerMediaType.put(mediaType.getTypeSubtype(), engine);
      return engine;
   }

   private EntityNameResolver createEntityNamesResolver(MediaType mediaType) {
      if (mediaType.match(APPLICATION_PROTOSTREAM)) {
         return new ProtobufEntityNameResolver(ctx);
      } else {
         ClassLoader classLoader = cr.getGlobalComponentRegistry().getComponent(ClassLoader.class);

         EntityNameResolver entityNameResolver;
         ReflectionEntityNamesResolver reflectionEntityNamesResolver = new ReflectionEntityNamesResolver(classLoader);
         if (isIndexed) {
            Set<Class<?>> knownClasses = cr.getComponent(QueryInterceptor.class).getKnownClasses();
            // If indexing is enabled, then use the known set of classes for lookup and the global classloader as a fallback.
            entityNameResolver = name -> knownClasses.stream()
                  .filter(c -> c.getName().equals(name))
                  .findFirst()
                  .orElse(reflectionEntityNamesResolver.resolve(name));
         } else {
            entityNameResolver = reflectionEntityNamesResolver;
         }
         return entityNameResolver;
      }
   }

}
