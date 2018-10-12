package org.infinispan.query.impl;

import static org.infinispan.query.impl.IndexPropertyInspector.getDataCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.getLockingCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.getMetadataCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.hasInfinispanDirectory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.hibernate.search.analyzer.definition.LuceneAnalysisDefinitionProvider;
import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.engine.service.classloading.spi.ClassLoaderService;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.SearchIntegratorBuilder;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.AggregatedClassLoader;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Transformer;
import org.infinispan.query.affinity.ShardAllocationManagerImpl;
import org.infinispan.query.affinity.ShardAllocatorManager;
import org.infinispan.query.backend.IndexModificationStrategy;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.backend.QueryKnownClasses;
import org.infinispan.query.backend.SearchableCacheConfiguration;
import org.infinispan.query.backend.TxQueryInterceptor;
import org.infinispan.query.clustered.NodeTopDocs;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.clustered.commandworkers.QueryBox;
import org.infinispan.query.continuous.impl.ContinuousQueryResult;
import org.infinispan.query.continuous.impl.IckleContinuousQueryCacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryEngine;
import org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper;
import org.infinispan.query.dsl.embedded.impl.IckleCacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.IckleFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.query.impl.externalizers.LuceneBooleanQueryExternalizer;
import org.infinispan.query.impl.externalizers.LuceneBytesRefExternalizer;
import org.infinispan.query.impl.externalizers.LuceneFieldDocExternalizer;
import org.infinispan.query.impl.externalizers.LuceneFuzzyQueryExternalizer;
import org.infinispan.query.impl.externalizers.LuceneMatchAllQueryExternalizer;
import org.infinispan.query.impl.externalizers.LucenePrefixQueryExternalizer;
import org.infinispan.query.impl.externalizers.LuceneScoreDocExternalizer;
import org.infinispan.query.impl.externalizers.LuceneSortExternalizer;
import org.infinispan.query.impl.externalizers.LuceneSortFieldExternalizer;
import org.infinispan.query.impl.externalizers.LuceneTermExternalizer;
import org.infinispan.query.impl.externalizers.LuceneTermQueryExternalizer;
import org.infinispan.query.impl.externalizers.LuceneTopDocsExternalizer;
import org.infinispan.query.impl.externalizers.LuceneTopFieldDocsExternalizer;
import org.infinispan.query.impl.externalizers.LuceneWildcardQueryExternalizer;
import org.infinispan.query.impl.massindex.DistributedExecutorMassIndexer;
import org.infinispan.query.impl.massindex.IndexWorker;
import org.infinispan.query.logging.Log;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.registry.InternalCacheRegistry.Flag;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * Lifecycle of the Query module: initializes the Hibernate Search engine and shuts it down at cache stop. Each cache
 * manager has its own instance of this class during its lifetime.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
@MetaInfServices(org.infinispan.lifecycle.ModuleLifecycle.class)
public class LifecycleManager implements ModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   /**
    * Caching the looked-up MBeanServer for the lifetime of the cache manager is safe.
    */
   private MBeanServer mbeanServer;

   /**
    * Registers the Search interceptor in the cache before it gets started
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName) || icr.internalCacheHasFlag(cacheName, Flag.QUERYABLE)) {
         AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
         ClassLoader aggregatedClassLoader = makeAggregatedClassLoader(cr.getGlobalComponentRegistry().getGlobalConfiguration().classLoader());
         SearchIntegrator searchFactory = null;
         boolean isIndexed = cfg.indexing().index().isEnabled();
         if (isIndexed) {
            cr.registerComponent(new ShardAllocationManagerImpl(), ShardAllocatorManager.class);
            searchFactory = createSearchIntegrator(cfg.indexing(), cr, aggregatedClassLoader);

            KeyTransformationHandler keyTransformationHandler = new KeyTransformationHandler(aggregatedClassLoader);
            cr.registerComponent(keyTransformationHandler, KeyTransformationHandler.class);

            createQueryInterceptorIfNeeded(cr.getComponent(BasicComponentRegistry.class), cfg, cache, searchFactory, keyTransformationHandler);
            addCacheDependencyIfNeeded(cacheName, cache.getCacheManager(), cfg.indexing());

            cr.registerComponent(new QueryBox(), QueryBox.class);
         }

         registerMatcher(cr, searchFactory, aggregatedClassLoader);

         cr.registerComponent(new EmbeddedQueryEngine(cache, isIndexed), EmbeddedQueryEngine.class);
      }
   }

   private void registerMatcher(ComponentRegistry cr, SearchIntegrator searchFactory, ClassLoader classLoader) {
      ReflectionMatcher reflectionMatcher = searchFactory == null ? new ReflectionMatcher(classLoader) :
            new ReflectionMatcher(new HibernateSearchPropertyHelper(searchFactory, new ReflectionEntityNamesResolver(classLoader)));
      cr.registerComponent(reflectionMatcher, ReflectionMatcher.class);
   }

   private void addCacheDependencyIfNeeded(String cacheStarting, EmbeddedCacheManager cacheManager, IndexingConfiguration indexingConfiguration) {
      if (indexingConfiguration.indexedEntities().isEmpty()) {
         // todo [anistor] remove dependency on QueryKnownClasses in Infinispan 10.0
         // indexed classes are autodetected and propagated across cluster via this cache
         cacheManager.addCacheDependency(cacheStarting, QueryKnownClasses.QUERY_KNOWN_CLASSES_CACHE_NAME);
      }
      if (hasInfinispanDirectory(indexingConfiguration.properties())) {
         String metadataCacheName = getMetadataCacheName(indexingConfiguration.properties());
         String lockingCacheName = getLockingCacheName(indexingConfiguration.properties());
         String dataCacheName = getDataCacheName(indexingConfiguration.properties());
         if (!cacheStarting.equals(metadataCacheName) && !cacheStarting.equals(lockingCacheName) && !cacheStarting.equals(dataCacheName)) {
            cacheManager.addCacheDependency(cacheStarting, metadataCacheName);
            cacheManager.addCacheDependency(cacheStarting, lockingCacheName);
            cacheManager.addCacheDependency(cacheStarting, dataCacheName);
         }
      }
   }

   private void createQueryInterceptorIfNeeded(BasicComponentRegistry cr, Configuration cfg, AdvancedCache<?, ?> cache,
                                               SearchIntegrator searchIntegrator, KeyTransformationHandler keyTransformationHandler) {
      log.registeringQueryInterceptor(cache.getName());

      ComponentRef<QueryInterceptor> queryInterceptorRef = cr.getComponent(QueryInterceptor.class);
      if (queryInterceptorRef != null) {
         // could be already present when two caches share a config
         return;
      }

      ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues = new ConcurrentHashMap<>();
      IndexModificationStrategy indexingStrategy = IndexModificationStrategy.configuredStrategy(searchIntegrator, cfg);
      QueryInterceptor queryInterceptor = new QueryInterceptor(searchIntegrator, keyTransformationHandler, indexingStrategy, txOldValues, cache);

      for (Map.Entry<Class<?>, Class<?>> kt : cfg.indexing().keyTransformers().entrySet()) {
         keyTransformationHandler.registerTransformer(kt.getKey(), (Class<? extends Transformer>) kt.getValue());
      }

      // Interceptor registration not needed, core configuration handling
      // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!
      AsyncInterceptorChain ic = cr.getComponent(AsyncInterceptorChain.class).wired();

      ConfigurationBuilder builder = new ConfigurationBuilder().read(cfg);

      EntryWrappingInterceptor wrappingInterceptor = ic.findInterceptorExtending(EntryWrappingInterceptor.class);
      AsyncInterceptor lastLoadingInterceptor = ic.findInterceptorExtending(CacheLoaderInterceptor.class);
      if (lastLoadingInterceptor == null) {
         lastLoadingInterceptor = wrappingInterceptor;
      }

      ic.addInterceptorAfter(queryInterceptor, lastLoadingInterceptor.getClass());
      cr.registerComponent(QueryInterceptor.class, queryInterceptor, true);
      cr.addDynamicDependency(AsyncInterceptorChain.class.getName(), QueryInterceptor.class.getName());

      // add the interceptor to the configuration also
      builder.customInterceptors().addInterceptor()
             .interceptor(queryInterceptor).after(lastLoadingInterceptor.getClass());

      if (cfg.transaction().transactionMode().isTransactional()) {
         TxQueryInterceptor txQueryInterceptor = new TxQueryInterceptor(txOldValues, queryInterceptor);
         ic.addInterceptorBefore(txQueryInterceptor, wrappingInterceptor.getClass());
         cr.registerComponent(TxQueryInterceptor.class, txQueryInterceptor, true);
         cr.addDynamicDependency(AsyncInterceptorChain.class.getName(), TxQueryInterceptor.class.getName());

         // add the interceptor to the configuration also
         builder.customInterceptors().addInterceptor()
               .interceptor(txQueryInterceptor).before(wrappingInterceptor.getClass());
      }

      cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      IndexingConfiguration indexingConfiguration = configuration.indexing();
      if (!indexingConfiguration.index().isEnabled()) {
         if (verifyChainContainsQueryInterceptor(cr)) {
            throw new IllegalStateException("It was NOT expected to find the Query interceptor registered in the InterceptorChain as indexing was disabled, but it was found");
         }
         return;
      }
      if (!verifyChainContainsQueryInterceptor(cr)) {
         throw new IllegalStateException("It was expected to find the Query interceptor registered in the InterceptorChain but it wasn't found");
      }

      SearchIntegrator searchFactory = cr.getComponent(SearchIntegrator.class);
      if (!indexingConfiguration.indexedEntities().isEmpty()) {
         Properties indexingProperties = indexingConfiguration.properties();
         if (hasInfinispanDirectory(indexingProperties)) {
            String metadataCacheName = getMetadataCacheName(indexingProperties);
            String lockingCacheName = getLockingCacheName(indexingProperties);
            String dataCacheName = getDataCacheName(indexingProperties);
            if (cacheName.equals(dataCacheName) && (cacheName.equals(metadataCacheName) || cacheName.equals(lockingCacheName))) {
               // Infinispan Directory causes runtime circular dependencies so we need to postpone creation of indexes until all components are initialised
               Class<?>[] indexedEntities = indexingConfiguration.indexedEntities().toArray(new Class<?>[indexingConfiguration.indexedEntities().size()]);
               searchFactory.addClasses(indexedEntities);
               checkIndexableClasses(searchFactory, indexingConfiguration.indexedEntities());
            }
         } else {
            checkIndexableClasses(searchFactory, indexingConfiguration.indexedEntities());
         }
      }

      registerQueryMBeans(cr, configuration, searchFactory);
   }

   /**
    * Check that the indexable classes declared by the user are really indexable.
    */
   private void checkIndexableClasses(SearchIntegrator searchFactory, Set<Class<?>> indexedEntities) {
      for (Class<?> c : indexedEntities) {
         if (searchFactory.getIndexBinding(new PojoIndexedTypeIdentifier(c)) == null) {
            throw log.classNotIndexable(c.getName());
         }
      }
   }

   /**
    * Register query statistics and mass-indexer MBeans for a cache.
    */
   private void registerQueryMBeans(ComponentRegistry cr, Configuration cfg, SearchIntegrator searchIntegrator) {
      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      // Resolve MBean server instance
      GlobalJmxStatisticsConfiguration jmxConfig = cr.getGlobalComponentRegistry().getGlobalConfiguration().globalJmxStatistics();
      if (mbeanServer == null) {
         mbeanServer = JmxUtil.lookupMBeanServer(jmxConfig.mbeanServerLookup(), jmxConfig.properties());
      }

      // Resolve jmx domain to use for query MBeans
      String queryGroupName = getQueryGroupName(jmxConfig.cacheManagerName(), cache.getName());
      String jmxDomain = JmxUtil.buildJmxDomain(jmxConfig.domain(), mbeanServer, queryGroupName);

      // Register query statistics MBean, but only enable it if Infinispan config says so
      try {
         ObjectName statsObjName = new ObjectName(jmxDomain + ":" + queryGroupName + ",component=Statistics");

         InfinispanQueryStatisticsInfo stats = new InfinispanQueryStatisticsInfo(searchIntegrator, statsObjName);
         stats.setStatisticsEnabled(cfg.jmxStatistics().enabled());

         JmxUtil.registerMBean(stats, statsObjName, mbeanServer);

         cr.registerComponent(stats, InfinispanQueryStatisticsInfo.class);
      } catch (Exception e) {
         throw new CacheException("Unable to register query statistics MBean", e);
      }

      // Register mass indexer MBean, picking metadata from repo
      ManageableComponentMetadata massIndexerCompMetadata = cr.getGlobalComponentRegistry().getComponentMetadataRepo()
            .findComponentMetadata(MassIndexer.class)
            .toManageableComponentMetadata();
      try {
         KeyTransformationHandler keyTransformationHandler = ComponentRegistryUtils.getKeyTransformationHandler(cache);
         TimeService timeService = ComponentRegistryUtils.getTimeService(cache);
         // TODO: MassIndexer should be some kind of query cache component?
         DistributedExecutorMassIndexer massIndexer = new DistributedExecutorMassIndexer(cache, searchIntegrator, keyTransformationHandler, timeService);
         ResourceDMBean mbean = new ResourceDMBean(massIndexer, massIndexerCompMetadata);
         ObjectName massIndexerObjName = new ObjectName(jmxDomain + ":"
               + queryGroupName + ",component=" + massIndexerCompMetadata.getJmxObjectName());
         JmxUtil.registerMBean(mbean, massIndexerObjName, mbeanServer);
      } catch (Exception e) {
         throw new CacheException("Unable to create MassIndexer MBean", e);
      }
   }

   private String getQueryGroupName(String cacheManagerName, String cacheName) {
      return "type=Query,manager=" + ObjectName.quote(cacheManagerName) + ",cache=" + ObjectName.quote(cacheName);
   }

   private boolean verifyChainContainsQueryInterceptor(ComponentRegistry cr) {
      AsyncInterceptorChain interceptorChain = cr.getComponent(AsyncInterceptorChain.class);
      return interceptorChain != null && interceptorChain.containsInterceptorType(QueryInterceptor.class, true);
   }

   private SearchIntegrator createSearchIntegrator(IndexingConfiguration indexingConfiguration, ComponentRegistry cr, ClassLoader aggregatedClassLoader) {
      SearchIntegrator searchIntegrator = cr.getComponent(SearchIntegrator.class);
      if (searchIntegrator != null && !searchIntegrator.isStopped()) {
         // a paranoid check against an unlikely failure
         throw new IllegalStateException("SearchIntegrator already initialized!");
      }

      // load ProgrammaticSearchMappingProviders from classpath
      Collection<ProgrammaticSearchMappingProvider> programmaticSearchMappingProviders = new LinkedHashSet<>();
      programmaticSearchMappingProviders.add(new DefaultSearchMappingProvider());  // make sure our DefaultSearchMappingProvider is first
      programmaticSearchMappingProviders.addAll(ServiceFinder.load(ProgrammaticSearchMappingProvider.class, aggregatedClassLoader));

      // load LuceneAnalysisDefinitionProvider from classpath
      Collection<LuceneAnalysisDefinitionProvider> analyzerDefProviders = ServiceFinder.load(LuceneAnalysisDefinitionProvider.class, aggregatedClassLoader);

      // Set up the search factory for Hibernate Search first.
      SearchConfiguration config = new SearchableCacheConfiguration(indexingConfiguration.indexedEntities(),
            indexingConfiguration.properties(), programmaticSearchMappingProviders, analyzerDefProviders, cr, aggregatedClassLoader);

      searchIntegrator = new SearchIntegratorBuilder().configuration(config).buildSearchIntegrator();
      cr.registerComponent(searchIntegrator, SearchIntegrator.class);
      return searchIntegrator;
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

      // add Hibernate Search's CL
      classLoaders.add(ClassLoaderService.class.getClassLoader());

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

      SearchIntegrator searchIntegrator = cr.getComponent(SearchIntegrator.class);
      if (searchIntegrator != null) {
         searchIntegrator.close();
      }

      unregisterQueryMBeans(cr, cacheName);
   }

   /**
    * Unregister query related MBeans for a cache, primarily the statistics, but also all other MBeans from the same
    * related group.
    */
   private void unregisterQueryMBeans(ComponentRegistry cr, String cacheName) {
      if (mbeanServer != null) {
         try {
            InfinispanQueryStatisticsInfo stats = cr.getComponent(InfinispanQueryStatisticsInfo.class);
            if (stats != null) {
               GlobalJmxStatisticsConfiguration jmxConfig = cr.getGlobalComponentRegistry().getGlobalConfiguration().globalJmxStatistics();
               String queryGroupName = getQueryGroupName(jmxConfig.cacheManagerName(), cacheName);
               String queryMBeanFilter = stats.getObjectName().getDomain() + ":" + queryGroupName + ",*";
               JmxUtil.unregisterMBeans(queryMBeanFilter, mbeanServer);
            }
         } catch (Exception e) {
            throw new CacheException("Unable to unregister query MBeans", e);
         }
      }
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      Configuration cfg = cr.getComponent(Configuration.class);
      removeQueryInterceptorFromConfiguration(cfg);
   }

   private void removeQueryInterceptorFromConfiguration(Configuration cfg) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      CustomInterceptorsConfigurationBuilder customInterceptorsBuilder = builder.customInterceptors();

      for (InterceptorConfiguration interceptorConfig : cfg.customInterceptors().interceptors()) {
         if (!(interceptorConfig.asyncInterceptor() instanceof QueryInterceptor)) {
            customInterceptorsBuilder.addInterceptor().read(interceptorConfig);
         }
      }

      cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      gcr.registerComponent(new QueryCache(), QueryCache.class);

      Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.ICKLE_FILTER_AND_CONVERTER, new IckleFilterAndConverter.IckleFilterAndConverterExternalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_FILTER_RESULT, new IckleFilterAndConverter.FilterResultExternalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_CACHE_EVENT_FILTER_CONVERTER, new IckleCacheEventFilterConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER, new IckleContinuousQueryCacheEventFilterConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_CONTINUOUS_QUERY_RESULT, new ContinuousQueryResult.Externalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_BOOLEAN, new LuceneBooleanQueryExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_TERM, new LuceneTermQueryExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_TERM, new LuceneTermExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_SORT, new LuceneSortExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_SORT_FIELD, new LuceneSortFieldExternalizer());
      externalizerMap.put(ExternalizerIds.CLUSTERED_QUERY_TOPDOCS, new NodeTopDocs.Externalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_TOPDOCS, new LuceneTopDocsExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_FIELD_SCORE_DOC, new LuceneFieldDocExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_SCORE_DOC, new LuceneScoreDocExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_TOPFIELDDOCS, new LuceneTopFieldDocsExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_MATCH_ALL, new LuceneMatchAllQueryExternalizer());
      externalizerMap.put(ExternalizerIds.INDEX_WORKER, new IndexWorker.Externalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_BYTES_REF, new LuceneBytesRefExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_PREFIX, new LucenePrefixQueryExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_WILDCARD, new LuceneWildcardQueryExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_FUZZY, new LuceneFuzzyQueryExternalizer());
      externalizerMap.put(ExternalizerIds.QUERY_DEFINITION, new QueryDefinition.Externalizer());
      externalizerMap.put(ExternalizerIds.CLUSTERED_QUERY_COMMAND_RESPONSE, new QueryResponse.Externalizer());
   }

   @Override
   public void cacheManagerStopped(GlobalComponentRegistry gcr) {
      mbeanServer = null;
   }
}
