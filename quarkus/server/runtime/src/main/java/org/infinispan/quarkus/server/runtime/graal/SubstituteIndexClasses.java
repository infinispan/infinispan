package org.infinispan.quarkus.server.runtime.graal;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexingConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.graalvm.substitutions.graal.Util;
import org.infinispan.query.concurrent.QueryPackageImpl;
import org.infinispan.query.dsl.embedded.impl.ObjectReflectionMatcher;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.embedded.impl.SearchQueryMaker;
import org.infinispan.query.dsl.embedded.impl.SearchQueryParsingResult;
import org.infinispan.query.impl.LifecycleManager;
import org.infinispan.query.impl.massindex.IndexWorker;
import org.infinispan.query.remote.impl.LazySearchMapping;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.model.impl.InfinispanBootstrapIntrospector;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

//import org.infinispan.lucene.LifecycleCallbacks;

class SubstituteIndexClasses {
}

@TargetClass(IndexWorker.class)
final class Target_IndexWorker {
   @Substitute
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      throw Util.unsupportedOperationException("Indexing");
   }
}

@TargetClass(LifecycleManager.class)
final class Target_org_infinispan_query_impl_LifecycleManager {
   @Substitute
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      // Do nothing - this method used to setup indexing parts and JMX (neither which are supported)
   }

   @Substitute
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      if (cfg.indexing().enabled()) {
         Util.unsupportedOperationException("Indexing", "Cache " + cacheName + " has it enabled!");
      }

      InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName) || icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.QUERYABLE)) {
         AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
         cr.registerComponent(ObjectReflectionMatcher.create(cache, new ReflectionEntityNamesResolver(getClass().getClassLoader()),null), ObjectReflectionMatcher.class);
         cr.registerComponent(new QueryEngine<>(cache, false), QueryEngine.class);
      }
   }
}

@TargetClass(org.infinispan.query.remote.impl.LifecycleManager.class)
final class Target_org_infinispan_query_remote_impl_LifecycleManager {

   @Substitute
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      // no-op
   }

   @Substitute
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      // no-op
   }

   @Substitute
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
   }
}

@TargetClass(IndexingConfigurationBuilder.class)
final class Target_IndexingConfigurationBuilder {
   @Alias
   private AttributeSet attributes;

   @Substitute
   public Object addIndexedEntity(Class<?> indexedEntity) {
      throw Util.unsupportedOperationException("Indexing");
   }

   @Substitute
   private Set<Class<?>> indexedEntities() {
      return Collections.emptySet();
   }
}

@TargetClass(SearchQueryMaker.class)
final class Target_SearchQueryMaker {
   @Substitute
   private boolean isMultiTermText(PropertyPath<?> propertyPath, String text) {
      return false;
   }

   @Substitute
   public SearchQueryParsingResult transform(IckleParsingResult<?> parsingResult, Map<String, Object> namedParameters,
                                             Class<?> targetedType, String targetedTypeName) {
      return null;
   }
}

@TargetClass(SearchMappingBuilder.class)
final class Target_SearchMappingBuilder {
   @Substitute
   public static InfinispanBootstrapIntrospector introspector(MethodHandles.Lookup lookup) {
      return null;
   }
}

@TargetClass(QueryPackageImpl.class)
final class Target_QueryPackageImpl {
   @Substitute
   public static void registerMetadata(ModuleMetadataBuilder.ModuleBuilder builder) {
      // no-op
   }
}

@TargetClass(LazySearchMapping.class)
@Delete
final class Target_LazySearchMapping {
}
