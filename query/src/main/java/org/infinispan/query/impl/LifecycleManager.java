package org.infinispan.query.impl;

import static org.infinispan.query.impl.IndexPropertyInspector.getDataCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.getLockingCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.getMetadataCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.hasInfinispanDirectory;
import static org.infinispan.query.impl.IndexPropertyInspector.isInfinispanDirectoryInternalCache;
import static org.infinispan.query.impl.SegmentFilterFactory.SEGMENT_FILTER_NAME;
import static org.infinispan.query.logging.Log.CONTAINER;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.management.ObjectName;

import org.apache.lucene.search.BooleanQuery;
import org.hibernate.search.analyzer.definition.LuceneAnalysisDefinitionProvider;
import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.engine.impl.MutableSearchFactory;
import org.hibernate.search.engine.service.classloading.spi.ClassLoaderService;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.SearchIntegratorBuilder;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.AggregatedClassLoader;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Transformer;
import org.infinispan.query.backend.IndexModificationStrategy;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.backend.SearchableCacheConfiguration;
import org.infinispan.query.backend.TxQueryInterceptor;
import org.infinispan.query.clustered.ClusteredQueryOperation;
import org.infinispan.query.clustered.NodeTopDocs;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.clustered.commandworkers.QueryBox;
import org.infinispan.query.dsl.embedded.impl.ObjectReflectionMatcher;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.query.impl.externalizers.FullTextFilterExternalizer;
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
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.registry.InternalCacheRegistry.Flag;
import org.infinispan.transaction.xa.GlobalTransaction;

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

   private static boolean maxBooleanClausesWasSet = false;

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
            setBooleanQueryMaxClauseCount(cfg.indexing().properties());

            searchFactory = createSearchIntegrator(cfg.indexing(), cr, aggregatedClassLoader);

            KeyTransformationHandler keyTransformationHandler = new KeyTransformationHandler(aggregatedClassLoader);
            cr.registerComponent(keyTransformationHandler, KeyTransformationHandler.class);

            createQueryInterceptorIfNeeded(cr, cfg, cache, searchFactory, keyTransformationHandler);
            addCacheDependencyIfNeeded(cacheName, cache.getCacheManager(), cfg.indexing());

            cr.registerComponent(new QueryBox(), QueryBox.class);
            DistributedExecutorMassIndexer massIndexer = new DistributedExecutorMassIndexer(cache, searchFactory,
                  keyTransformationHandler, ComponentRegistryUtils.getTimeService(cache));
            cr.registerComponent(massIndexer, MassIndexer.class);
         }

         cr.registerComponent(ObjectReflectionMatcher.create(new ReflectionEntityNamesResolver(aggregatedClassLoader), searchFactory), ObjectReflectionMatcher.class);
         cr.registerComponent(new QueryEngine<>(cache, isIndexed), QueryEngine.class);
      }
   }

   private void addCacheDependencyIfNeeded(String cacheStarting, EmbeddedCacheManager cacheManager, IndexingConfiguration indexingConfiguration) {
      if (hasInfinispanDirectory(indexingConfiguration.properties())) {
         String metadataCacheName = getMetadataCacheName(indexingConfiguration.properties());
         if (!metadataCacheName.equals(cacheStarting)) {
            SecurityActions.addCacheDependency(cacheManager, cacheStarting, metadataCacheName);
         }

         String lockingCacheName = getLockingCacheName(indexingConfiguration.properties());
         if (!lockingCacheName.equals(cacheStarting)) {
            SecurityActions.addCacheDependency(cacheManager, cacheStarting, lockingCacheName);
         }

         String dataCacheName = getDataCacheName(indexingConfiguration.properties());
         if (!dataCacheName.equals(cacheStarting)) {
            SecurityActions.addCacheDependency(cacheManager, cacheStarting, dataCacheName);
         }
      }
   }

   private void createQueryInterceptorIfNeeded(ComponentRegistry cr, Configuration cfg, AdvancedCache<?, ?> cache,
                                               SearchIntegrator searchIntegrator, KeyTransformationHandler keyTransformationHandler) {
      CONTAINER.registeringQueryInterceptor(cache.getName());

      BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
      ComponentRef<QueryInterceptor> queryInterceptorRef = bcr.getComponent(QueryInterceptor.class);
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

      AsyncInterceptorChain ic = bcr.getComponent(AsyncInterceptorChain.class).wired();

      EntryWrappingInterceptor wrappingInterceptor = ic.findInterceptorExtending(EntryWrappingInterceptor.class);
      AsyncInterceptor lastLoadingInterceptor = ic.findInterceptorExtending(CacheLoaderInterceptor.class);
      if (lastLoadingInterceptor == null) {
         lastLoadingInterceptor = wrappingInterceptor;
      }

      ic.addInterceptorAfter(queryInterceptor, lastLoadingInterceptor.getClass());
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
      Properties indexingProperties = indexingConfiguration.properties();
      if (isInfinispanDirectoryInternalCache(cacheName, indexingProperties)) {
         // Infinispan Directory causes runtime circular dependencies so we have postponed creation of indexes until
         // all components and involved caches are initialised (see SearchableCacheConfiguration). Now is the right time!
         // TODO classes defined programmatically via SearchMapping are lost!
         // Workaround: Remove pre-registered filter to avoid SearchException when dynamically adding entities
         MutableSearchFactory mutableSearchFactory = searchFactory.unwrap(MutableSearchFactory.class);
         mutableSearchFactory.getFilterDefinitions().remove(SEGMENT_FILTER_NAME);
         Class<?>[] indexedEntities = indexingConfiguration.indexedEntities().toArray(new Class<?>[0]);
         for(Class<?> clazz : indexedEntities) {
            mutableSearchFactory.getProgrammaticMapping().entity(clazz).classBridge(SegmentFieldBridge.class);
         }
         searchFactory.addClasses(indexedEntities);
      }

      checkIndexableClasses(searchFactory, indexingConfiguration.indexedEntities());

      registerQueryMBeans(cr, configuration, searchFactory);
   }

   /**
    * Check that the indexable classes declared by the user are really indexable by looking at the presence of Hibernate
    * Search index bindings.
    */
   private void checkIndexableClasses(SearchIntegrator searchFactory, Set<Class<?>> indexedEntities) {
      for (Class<?> c : indexedEntities) {
         if (searchFactory.getIndexBinding(new PojoIndexedTypeIdentifier(c)) == null) {
            throw CONTAINER.classNotIndexable(c.getName());
         }
      }
   }

   /**
    * Register query statistics and mass-indexer MBeans for a cache.
    */
   private void registerQueryMBeans(ComponentRegistry cr, Configuration cfg, SearchIntegrator searchIntegrator) {
      GlobalConfiguration globalConfig = cr.getGlobalComponentRegistry().getGlobalConfiguration();
      if (!globalConfig.statistics())
         return;

      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      String queryGroupName = getQueryGroupName(globalConfig.cacheManagerName(), cache.getName());
      CacheJmxRegistration jmxRegistration = cr.getComponent(CacheJmxRegistration.class);

      MassIndexer massIndexer = ComponentRegistryUtils.getMassIndexer(cache);

      // Register query statistics MBean, but only enable it if Infinispan config says so
      try {
         InfinispanQueryStatisticsInfo stats = new InfinispanQueryStatisticsInfo(searchIntegrator, massIndexer);
         stats.setStatisticsEnabled(cfg.jmxStatistics().enabled());
         cr.registerComponent(stats, InfinispanQueryStatisticsInfo.class);
         jmxRegistration.registerMBean(stats, queryGroupName);
      } catch (Exception e) {
         throw new CacheException("Unable to register query statistics MBean", e);
      }

      // Register mass indexer MBean
      try {
         jmxRegistration.registerMBean(massIndexer, queryGroupName);
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

      programmaticSearchMappingProviders.add((cache, mapping) -> {
         for (Class<?> indexedEntity : indexingConfiguration.indexedEntities()) {
            mapping.entity(indexedEntity).classBridge(SegmentFieldBridge.class);
         }
      });

      // load LuceneAnalysisDefinitionProvider from classpath
      Collection<LuceneAnalysisDefinitionProvider> analyzerDefProviders = ServiceFinder.load(LuceneAnalysisDefinitionProvider.class, aggregatedClassLoader);

      // Set up the search factory for Hibernate Search first.
      SearchConfiguration searchConfiguration = new SearchableCacheConfiguration(indexingConfiguration.indexedEntities(),
            indexingConfiguration.properties(), programmaticSearchMappingProviders, analyzerDefProviders, cr, aggregatedClassLoader);

      searchIntegrator = new SearchIntegratorBuilder().configuration(searchConfiguration).buildSearchIntegrator();
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
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());

      Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
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
      externalizerMap.put(ExternalizerIds.FULL_TEXT_FILTER, new FullTextFilterExternalizer());
      externalizerMap.put(ExternalizerIds.CLUSTERED_QUERY_OPERATION, new ClusteredQueryOperation.Externalizer());
   }

   /**
    * Sets {@link BooleanQuery#setMaxClauseCount} according to the value of {@link #MAX_BOOLEAN_CLAUSES_SYS_PROP} system
    * property. This is executed only once, when first indexed cache is started.
    *
    * @param properties
    */
   private void setBooleanQueryMaxClauseCount(TypedProperties properties) {
      if (!maxBooleanClausesWasSet) {
         maxBooleanClausesWasSet = true;
         String maxClauseCountProp = properties.getProperty(MAX_BOOLEAN_CLAUSES_SYS_PROP);
         if (maxClauseCountProp == null) {
            maxClauseCountProp = SecurityActions.getSystemProperty(MAX_BOOLEAN_CLAUSES_SYS_PROP);
         }
         if (maxClauseCountProp != null) {
            int maxClauseCount;
            try {
               maxClauseCount = Integer.parseInt(maxClauseCountProp);
            } catch (NumberFormatException e) {
               CONTAINER.failedToParseSystemProperty(MAX_BOOLEAN_CLAUSES_SYS_PROP, e);
               throw e;
            }
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
}
