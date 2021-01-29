package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.dsl.embedded.impl.ObjectReflectionMatcher;
import org.infinispan.search.mapper.mapping.SearchMapping;

/**
 * Implementation of {@link RemoteQueryManager} for caches storing deserialized content (Java Objects).
 *
 * @since 9.4
 */
final class ObjectRemoteQueryManager extends BaseRemoteQueryManager {

   private final Map<String, ObjectRemoteQueryEngine> enginePerMediaType = new ConcurrentHashMap<>();

   private final SerializationContext serCtx;

   private final SearchMapping searchMapping;

   private final ComponentRegistry cr;

   ObjectRemoteQueryManager(AdvancedCache<?, ?> cache, ComponentRegistry cr, QuerySerializers querySerializers) {
      super(cache, querySerializers, cr);
      this.cr = cr;
      this.searchMapping = cr.getComponent(SearchMapping.class);
      this.serCtx = SecurityActions.getSerializationContext(cache.getCacheManager());

      BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
      ObjectReflectionMatcher objectReflectionMatcher = ObjectReflectionMatcher.create(
            createEntityNamesResolver(APPLICATION_OBJECT), searchMapping);
      bcr.replaceComponent(ObjectReflectionMatcher.class.getName(), objectReflectionMatcher, true);
      bcr.rewire();

      ProtobufObjectReflectionMatcher protobufObjectReflectionMatcher = ProtobufObjectReflectionMatcher.create(createEntityNamesResolver(APPLICATION_PROTOSTREAM), serCtx);
      bcr.registerComponent(ProtobufObjectReflectionMatcher.class, protobufObjectReflectionMatcher, true);
   }

   @Override
   public Class<? extends Matcher> getMatcherClass(MediaType mediaType) {
      return getQueryEngineForMediaType(mediaType).getMatcherClass();
   }

   @Override
   public ObjectRemoteQueryEngine getQueryEngine(AdvancedCache<?, ?> cache) {
      return getQueryEngineForMediaType(cache.getValueDataConversion().getRequestMediaType());
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      return filterResult;
   }

   private ObjectRemoteQueryEngine getQueryEngineForMediaType(MediaType mediaType) {
      ObjectRemoteQueryEngine queryEngine = enginePerMediaType.get(mediaType.getTypeSubtype());
      if (queryEngine == null) {
         ReflectionMatcher matcher = mediaType.match(APPLICATION_PROTOSTREAM) ? cr.getComponent(ProtobufObjectReflectionMatcher.class) :
               cr.getComponent(ObjectReflectionMatcher.class);

         queryEngine = new ObjectRemoteQueryEngine(cache, searchMapping != null, matcher.getClass());
         enginePerMediaType.put(mediaType.getTypeSubtype(), queryEngine);
      }
      return queryEngine;
   }

   private EntityNameResolver<Class<?>> createEntityNamesResolver(MediaType mediaType) {
      if (mediaType.match(APPLICATION_PROTOSTREAM)) {
         return new ProtobufEntityNameResolver(serCtx);
      } else {
         ClassLoader classLoader = cr.getGlobalComponentRegistry().getComponent(ClassLoader.class);
         ReflectionEntityNamesResolver reflectionEntityNamesResolver = new ReflectionEntityNamesResolver(classLoader);
         if (searchMapping != null) {
            // If indexing is enabled then use the declared set of indexed classes for lookup but fallback to global classloader.
            QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
            Map<String, Class<?>> indexedEntities = queryInterceptor.indexedEntities();
            return name -> {
               Class<?> c = indexedEntities.get(name);
               if (c == null) {
                  c = reflectionEntityNamesResolver.resolve(name);
               }
               return c;
            };
         }
         return reflectionEntityNamesResolver;
      }
   }
}
