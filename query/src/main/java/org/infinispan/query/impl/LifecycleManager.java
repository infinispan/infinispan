package org.infinispan.query.impl;

import static org.infinispan.query.core.impl.Log.CONTAINER;
import static org.infinispan.query.impl.config.SearchPropertyExtractor.extractProperties;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.management.ObjectName;

import org.apache.lucene.search.BooleanQuery;
import org.hibernate.search.backend.lucene.work.spi.LuceneWorkExecutorProvider;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.QueryProducer;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.AggregatedClassLoader;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexShardingConfiguration;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.cache.IndexingMode;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.metrics.impl.CacheMetricsRegistration;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.Transformer;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.backend.TxQueryInterceptor;
import org.infinispan.query.core.QueryProducerImpl;
import org.infinispan.query.core.impl.QueryCache;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.embedded.impl.ObjectReflectionMatcher;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.massindex.DistributedExecutorMassIndexer;
import org.infinispan.query.stats.impl.LocalIndexStatistics;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.registry.InternalCacheRegistry.Flag;
import org.infinispan.search.mapper.mapping.ProgrammaticSearchMappingProvider;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.mapping.SearchMappingCommonBuilding;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;

/**
 * Lifecycle of the Query module: initializes the Hibernate Search engine and shuts it down at cache stop. Each cache
 * manager has its own instance of this class during its lifetime.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
@InfinispanModule(name = "query", requiredModules = {"core", "query-core"}, optionalModules = "lucene-directory")
public class LifecycleManager implements ModuleLifecycle {

   /**
    * Optional integer system property that sets value of {@link BooleanQuery#setMaxClauseCount}.
    */
   public static final String MAX_BOOLEAN_CLAUSES_SYS_PROP = "infinispan.query.lucene.max-boolean-clauses";

   private volatile boolean remoteQueryEnabled = false;

   /**
    * Registers the Search interceptor in the cache before it gets started
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      StorageConfigurationManager scm = cr.getComponent(StorageConfigurationManager.class);
      LocalQueryStatistics queryStatistics = cr.getComponent(LocalQueryStatistics.class);
      if (!icr.isInternalCache(cacheName) || icr.internalCacheHasFlag(cacheName, Flag.QUERYABLE)) {
         AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
         SecurityActions.addCacheDependency(cache.getCacheManager(), cacheName, QueryCache.QUERY_CACHE_NAME);

         ClassLoader aggregatedClassLoader = makeAggregatedClassLoader(cr.getGlobalComponentRegistry().getGlobalConfiguration().classLoader());
         boolean isIndexed = cfg.indexing().enabled();

         SearchMapping searchMapping = null;
         if (isIndexed) {
            boolean useJavaEmbeddedEntities = cfg.indexing().useJavaEmbeddedEntities();

            Map<String, Class<?>> indexedClasses;
            if (!scm.getValueStorageMediaType().match(MediaType.APPLICATION_PROTOSTREAM) || !remoteQueryEnabled ||
                  useJavaEmbeddedEntities) {
               indexedClasses = makeIndexedClassesMap(cache);
            } else {
               indexedClasses = Collections.emptyMap();
            }

            KeyTransformationHandler keyTransformationHandler = new KeyTransformationHandler(aggregatedClassLoader);
            cr.registerComponent(keyTransformationHandler, KeyTransformationHandler.class);
            for (Map.Entry<Class<?>, Class<?>> kt : cfg.indexing().keyTransformers().entrySet()) {
               keyTransformationHandler.registerTransformer(kt.getKey(), (Class<? extends Transformer>) kt.getValue());
            }

            searchMapping = createSearchMapping(queryStatistics, cfg.indexing(), indexedClasses, cr, cache,
                  keyTransformationHandler, aggregatedClassLoader);

            createQueryInterceptorIfNeeded(cr, cfg, cache, indexedClasses);

            Indexer massIndexer = new DistributedExecutorMassIndexer(cache);
            cr.registerComponent(massIndexer, Indexer.class);
            if (searchMapping != null) {
               BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
               bcr.replaceComponent(IndexStatistics.class.getName(), new LocalIndexStatistics(), true);
               bcr.rewire();

               IndexStartupRunner.run(searchMapping, massIndexer, cfg);
            }
         }

         cr.registerComponent(ObjectReflectionMatcher.create(
               new ReflectionEntityNamesResolver(aggregatedClassLoader), searchMapping),
               ObjectReflectionMatcher.class);
         QueryEngine<Object> engine = new QueryEngine<>(cache, isIndexed);
         cr.registerComponent(engine, QueryEngine.class);

         // the cast ithis is the only implementation we have
         @SuppressWarnings("unchecked")
         QueryProducerImpl queryProducer = (QueryProducerImpl) cr.getComponent(QueryProducer.class);
         if (queryProducer != null) {
            queryProducer.upgrade(engine);
         } else {
            cr.registerComponent(new QueryProducerImpl(engine), QueryProducer.class);
         }
      }
   }

   public void enableRemoteQuery() {
      remoteQueryEnabled = true;
   }

   private Map<String, Class<?>> makeIndexedClassesMap(AdvancedCache<?, ?> cache) {
      Configuration cacheConfiguration = cache.getCacheConfiguration();
      Map<String, Class<?>> entities = new HashMap<>();
      // Try to resolve the indexed type names to class names.
      for (String entityName : cacheConfiguration.indexing().indexedEntityTypes()) {
         // include classes declared in indexing config
         try {
            entities.put(entityName, Util.loadClass(entityName, cache.getClassLoader()));
         } catch (Exception e) {
            throw CONTAINER.cannotLoadIndexedClass(entityName, e);
         }
      }
      return entities;
   }

   private void createQueryInterceptorIfNeeded(ComponentRegistry cr, Configuration cfg, AdvancedCache<?, ?> cache,
                                               Map<String, Class<?>> indexedClasses) {
      CONTAINER.debugf("Registering Query interceptor for cache %s", cache.getName());

      BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
      ComponentRef<QueryInterceptor> queryInterceptorRef = bcr.getComponent(QueryInterceptor.class);
      if (queryInterceptorRef != null) {
         // could be already present when two caches share a config
         return;
      }

      ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues = new ConcurrentHashMap<>();
      boolean manualIndexing = cfg.indexing().indexingMode().equals(IndexingMode.MANUAL);

      QueryInterceptor queryInterceptor = new QueryInterceptor(manualIndexing, txOldValues, cache, indexedClasses);

      AsyncInterceptorChain ic = bcr.getComponent(AsyncInterceptorChain.class).wired();

      EntryWrappingInterceptor wrappingInterceptor = ic.findInterceptorExtending(EntryWrappingInterceptor.class);

      ic.addInterceptorBefore(queryInterceptor, wrappingInterceptor.getClass());
      bcr.registerComponent(QueryInterceptor.class, queryInterceptor, true);
      bcr.addDynamicDependency(AsyncInterceptorChain.class.getName(), QueryInterceptor.class.getName());

      if (cfg.transaction().transactionMode().isTransactional()) {
         TxQueryInterceptor txQueryInterceptor = new TxQueryInterceptor(txOldValues, queryInterceptor);
         ic.addInterceptorBefore(txQueryInterceptor, wrappingInterceptor.getClass());
         bcr.registerComponent(TxQueryInterceptor.class, txQueryInterceptor, true);
         bcr.addDynamicDependency(AsyncInterceptorChain.class.getName(), TxQueryInterceptor.class.getName());
      }
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      StorageConfigurationManager scm = cr.getComponent(StorageConfigurationManager.class);
      IndexingConfiguration indexingConfiguration = configuration.indexing();
      if (!indexingConfiguration.enabled()) {
         if (verifyChainContainsQueryInterceptor(cr)) {
            throw new IllegalStateException("It was NOT expected to find the Query interceptor registered in the InterceptorChain as indexing was disabled, but it was found");
         }
         return;
      }
      if (!verifyChainContainsQueryInterceptor(cr)) {
         throw new IllegalStateException("It was expected to find the Query interceptor registered in the InterceptorChain but it wasn't found");
      }

      SearchMapping searchMapping = cr.getComponent(SearchMapping.class);
      if (searchMapping != null && !scm.getValueStorageMediaType().match(MediaType.APPLICATION_PROTOSTREAM)) {
         checkIndexableClasses(searchMapping, indexingConfiguration.indexedEntityTypes(), cr.getGlobalComponentRegistry().getGlobalConfiguration().classLoader());
      }

      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      Indexer massIndexer = ComponentRegistryUtils.getIndexer(cache);
      InfinispanQueryStatisticsInfo stats = new InfinispanQueryStatisticsInfo(Search.getSearchStatistics(cache), SecurityActions.getCacheComponentRegistry(cache).getComponent(Authorizer.class));
      cr.registerComponent(stats, InfinispanQueryStatisticsInfo.class);

      registerQueryMBeans(cr, massIndexer, stats);

      registerMetrics(cr, stats);
   }

   private void registerMetrics(ComponentRegistry cr, InfinispanQueryStatisticsInfo stats) {
      CacheMetricsRegistration cacheMetricsRegistration = cr.getComponent(CacheMetricsRegistration.class);
      if (cacheMetricsRegistration.metricsEnabled()) {
         cacheMetricsRegistration.registerMetrics(stats, "query", "statistics");
      }
   }

   /**
    * Check that the indexable classes declared by the user are really indexable by looking at the presence of Hibernate
    * Search index bindings.
    */
   private void checkIndexableClasses(SearchMapping searchMapping, Set<String> indexedEntities, ClassLoader classLoader) {
      if (indexedEntities.isEmpty()) {
         return;
      }
      for (String entityName : indexedEntities) {
         Class<?> indexedClass = Util.loadClass(entityName, classLoader);
         if (searchMapping.indexedEntity(indexedClass) == null) {
            throw Log.CONTAINER.classNotIndexable(entityName);
         }
      }
   }

   /**
    * Register query statistics and mass-indexer MBeans for a cache.
    */
   private void registerQueryMBeans(ComponentRegistry cr, Indexer massIndexer, InfinispanQueryStatisticsInfo stats) {
      GlobalConfiguration globalConfig = cr.getGlobalComponentRegistry().getGlobalConfiguration();
      if (globalConfig.jmx().enabled()) {
         Cache<?, ?> cache = cr.getComponent(Cache.class);
         String queryGroupName = getQueryGroupName(globalConfig.cacheManagerName(), cache.getName());
         CacheJmxRegistration jmxRegistration = cr.getComponent(CacheJmxRegistration.class);
         try {
            jmxRegistration.registerMBean(stats, queryGroupName);
         } catch (Exception e) {
            throw new CacheException("Unable to register query statistics MBean", e);
         }
         try {
            jmxRegistration.registerMBean(massIndexer, queryGroupName);
         } catch (Exception e) {
            throw new CacheException("Unable to register MassIndexer MBean", e);
         }
      }
   }

   private String getQueryGroupName(String cacheManagerName, String cacheName) {
      return "type=Query,manager=" + ObjectName.quote(cacheManagerName) + ",cache=" + ObjectName.quote(cacheName);
   }

   private boolean verifyChainContainsQueryInterceptor(ComponentRegistry cr) {
      AsyncInterceptorChain interceptorChain = cr.getComponent(AsyncInterceptorChain.class);
      return interceptorChain != null && interceptorChain.containsInterceptorType(QueryInterceptor.class, true);
   }

   private SearchMapping createSearchMapping(LocalQueryStatistics queryStatistics,
                                             IndexingConfiguration indexingConfiguration,
                                             Map<String, Class<?>> indexedClasses, ComponentRegistry cr,
                                             AdvancedCache<?, ?> cache,
                                             KeyTransformationHandler keyTransformationHandler,
                                             ClassLoader aggregatedClassLoader) {
      SearchMapping searchMapping = cr.getComponent(SearchMapping.class);
      if (searchMapping != null && !searchMapping.isClose()) {
         // a paranoid check against an unlikely failure
         throw new IllegalStateException("SearchIntegrator already initialized!");
      }

      GlobalConfiguration globalConfiguration = cr.getGlobalComponentRegistry().getGlobalConfiguration();

      // load ProgrammaticSearchMappingProviders from classpath
      Collection<ProgrammaticSearchMappingProvider> mappingProviders =
            ServiceFinder.load(ProgrammaticSearchMappingProvider.class, aggregatedClassLoader);

      BlockingManager blockingManager = cr.getGlobalComponentRegistry().getComponent(BlockingManager.class);
      LuceneWorkExecutorProvider luceneWorkExecutorProvider =
            cr.getGlobalComponentRegistry().getComponent(LuceneWorkExecutorProvider.class);

      Integer numberOfShards = 1;
      IndexShardingConfiguration sharding = indexingConfiguration.sharding();
      if (sharding != null) {
         numberOfShards = sharding.getShards();
      }

      SearchMappingCommonBuilding commonBuilding = new SearchMappingCommonBuilding(
            KeyTransformationHandlerIdentifierBridge.createReference(keyTransformationHandler),
            extractProperties(globalConfiguration, cache.getName(), indexingConfiguration, aggregatedClassLoader),
            aggregatedClassLoader, mappingProviders, blockingManager, luceneWorkExecutorProvider,
            numberOfShards, new IndexerConfig(cache));
      Set<Class<?>> types = new HashSet<>(indexedClasses.values());

      if (!types.isEmpty()) {
         // use the common builder to create the mapping now
         SearchMappingBuilder builder = commonBuilding.builder(SearchMappingBuilder.introspector(MethodHandles.lookup()));
         builder.setEntityLoader(new EntityLoaderFactory<>(cache, queryStatistics));
         builder.addEntityTypes(types);
         searchMapping = builder.build(Optional.empty());
         cr.registerComponent(searchMapping, SearchMapping.class);
      }

      if (searchMapping == null) {
         // register the common builder to create the mapping at a later time
         cr.registerComponent(commonBuilding, SearchMappingCommonBuilding.class);
      }

      return searchMapping;
   }

   /**
    * Create a class loader that delegates loading to an ordered set of class loaders.
    *
    * @param globalClassLoader the cache manager's global ClassLoader from GlobalConfiguration
    * @return the aggregated ClassLoader
    */
   private ClassLoader makeAggregatedClassLoader(ClassLoader globalClassLoader) {
      // use an ordered set to deduplicate them
      Set<ClassLoader> classLoaders = new LinkedHashSet<>(6);

      // add the cache manager's CL
      if (globalClassLoader != null) {
         classLoaders.add(globalClassLoader);
      }

      // add Infinispan's CL
      classLoaders.add(AggregatedClassLoader.class.getClassLoader());

      // add this module's CL
      classLoaders.add(getClass().getClassLoader());

      // add the TCCL
      try {
         ClassLoader tccl = Thread.currentThread().getContextClassLoader();
         if (tccl != null) {
            classLoaders.add(tccl);
         }
      } catch (Exception e) {
         // ignored
      }

      // add the system CL
      try {
         ClassLoader syscl = ClassLoader.getSystemClassLoader();
         if (syscl != null) {
            classLoaders.add(syscl);
         }

      } catch (Exception e) {
         // ignored
      }

      return new AggregatedClassLoader(classLoaders);
   }

   @Override
   public void cacheStopping(ComponentRegistry cr, String cacheName) {
      QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
      if (queryInterceptor != null) {
         queryInterceptor.prepareForStopping();
      }

      SearchMapping searchMapping = cr.getComponent(SearchMapping.class);
      if (searchMapping != null) {
         searchMapping.close();
      }
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      QueryCache queryCache = cr.getComponent(QueryCache.class);
      if (queryCache != null) {
         InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
         if (!icr.isInternalCache(cacheName) || icr.internalCacheHasFlag(cacheName, Flag.QUERYABLE)) {
            queryCache.clear(cacheName);
         }
      }
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, new GlobalContextInitializerImpl());
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      setMaxBooleanClauses();
   }

   private void setMaxBooleanClauses() {
      Integer maxClauseCount = Integer.getInteger(MAX_BOOLEAN_CLAUSES_SYS_PROP);
      if (maxClauseCount != null) {
         int currentMaxClauseCount = BooleanQuery.getMaxClauseCount();
         if (maxClauseCount > currentMaxClauseCount) {
            CONTAINER.settingBooleanQueryMaxClauseCount(MAX_BOOLEAN_CLAUSES_SYS_PROP, maxClauseCount);
            BooleanQuery.setMaxClauseCount(maxClauseCount);
         } else {
            CONTAINER.ignoringBooleanQueryMaxClauseCount(MAX_BOOLEAN_CLAUSES_SYS_PROP, maxClauseCount, currentMaxClauseCount);
         }
      }
   }
}
