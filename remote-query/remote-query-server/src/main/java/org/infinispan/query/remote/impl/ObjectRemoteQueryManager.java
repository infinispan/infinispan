package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.search.spi.SearchIntegrator;
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

/**
 * Implementation of {@link RemoteQueryManager} for caches storing deserialized content (Java Objects).
 *
 * @since 9.4
 */
final class ObjectRemoteQueryManager extends BaseRemoteQueryManager {

   private final Map<String, ObjectRemoteQueryEngine> enginePerMediaType = new ConcurrentHashMap<>();

   private final SerializationContext serCtx;

   private final SearchIntegrator searchIntegrator;

   private final ComponentRegistry cr;

   ObjectRemoteQueryManager(AdvancedCache<?, ?> cache, ComponentRegistry cr, QuerySerializers querySerializers) {
      super(cache, querySerializers);
      this.cr = cr;
      this.searchIntegrator = cr.getComponent(SearchIntegrator.class);
      this.serCtx = SecurityActions.getSerializationContext(cache.getCacheManager());

      BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
      ObjectReflectionMatcher objectReflectionMatcher = ObjectReflectionMatcher.create(createEntityNamesResolver(APPLICATION_OBJECT), searchIntegrator);
      bcr.replaceComponent(ObjectReflectionMatcher.class.getName(), objectReflectionMatcher, true);

      ProtobufObjectReflectionMatcher protobufObjectReflectionMatcher = ProtobufObjectReflectionMatcher.create(createEntityNamesResolver(APPLICATION_PROTOSTREAM), serCtx, searchIntegrator);
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

         queryEngine = new ObjectRemoteQueryEngine(cache, searchIntegrator != null, matcher.getClass());
         enginePerMediaType.put(mediaType.getTypeSubtype(), queryEngine);
      }
      return queryEngine;
   }

   private EntityNameResolver createEntityNamesResolver(MediaType mediaType) {
      if (mediaType.match(APPLICATION_PROTOSTREAM)) {
         return new ProtobufEntityNameResolver(serCtx);
      } else {
         ClassLoader classLoader = cr.getGlobalComponentRegistry().getComponent(ClassLoader.class);
         ReflectionEntityNamesResolver reflectionEntityNamesResolver = new ReflectionEntityNamesResolver(classLoader);
         if (searchIntegrator != null) {
            // If indexing is enabled, then use the known set of classes for lookup and the global classloader as a fallback.
            QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
            return name -> queryInterceptor.getKnownClasses().stream()
                                           .filter(c -> c.getName().equals(name))
                                           .findFirst()
                                           .orElse(reflectionEntityNamesResolver.resolve(name));
         }
         return reflectionEntityNamesResolver;
      }
   }
}
