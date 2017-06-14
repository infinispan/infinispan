package org.infinispan.query.impl;

import static org.infinispan.query.impl.IndexPropertyInspector.getDataCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.getLockingCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.getMetadataCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.hasInfinispanDirectory;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.hibernate.search.cfg.Environment;
import org.hibernate.search.cfg.SearchMapping;
import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.SearchIntegratorBuilder;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.interceptors.impl.VersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.affinity.ShardAllocationManagerImpl;
import org.infinispan.query.affinity.ShardAllocatorManager;
import org.infinispan.query.backend.IndexModificationStrategy;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.backend.QueryKnownClasses;
import org.infinispan.query.backend.SearchableCacheConfiguration;
import org.infinispan.query.clustered.QueryBox;
import org.infinispan.query.continuous.impl.ContinuousQueryResult;
import org.infinispan.query.continuous.impl.IckleContinuousQueryCacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryEngine;
import org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper;
import org.infinispan.query.dsl.embedded.impl.IckleCacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.IckleFilterAndConverter;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.impl.externalizers.ClusteredTopDocsExternalizer;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.query.impl.externalizers.LuceneBooleanQueryExternalizer;
import org.infinispan.query.impl.externalizers.LuceneBytesRefExternalizer;
import org.infinispan.query.impl.externalizers.LuceneFieldDocExternalizer;
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
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * Lifecycle of the Query module: initializes the Hibernate Search engine and shuts it down
 * at cache stop.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
@MetaInfServices(org.infinispan.lifecycle.ModuleLifecycle.class)
@SuppressWarnings("unused")
public class LifecycleManager implements ModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   private static final Object REMOVED_REGISTRY_COMPONENT = new Object();

   private MBeanServer mbeanServer;

   private String jmxDomain;

   /**
    * Registers the Search interceptor in the cache before it gets started
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName) || icr.internalCacheHasFlag(cacheName, Flag.QUERYABLE)) {
         boolean isIndexed = cfg.indexing().index().isEnabled();
         AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();

         SearchIntegrator searchFactory = null;
         if (isIndexed) {
            log.registeringQueryInterceptor(cacheName);
            cr.registerComponent(new ShardAllocationManagerImpl(), ShardAllocatorManager.class);
            searchFactory = getSearchFactory(cacheName, cfg.indexing(), cr);
            createQueryInterceptorIfNeeded(cr, cfg, searchFactory);
            addCacheDependencyIfNeeded(cacheName, cache.getCacheManager(), cfg.indexing());

            // initializing the query module command initializer.
            // we can t inject Cache and CacheManager with @inject in there
            CommandInitializer initializer = cr.getComponent(CommandInitializer.class);
            initializer.setCacheManager(cache.getCacheManager());

            QueryBox queryBox = new QueryBox();
            queryBox.setCache(cache);
            cr.registerComponent(queryBox, QueryBox.class);
         }

         registerMatcher(cr, searchFactory);

         EmbeddedQueryEngine queryEngine = new EmbeddedQueryEngine(cache, isIndexed);
         cr.registerComponent(queryEngine, EmbeddedQueryEngine.class);
      }
   }

   private void registerMatcher(ComponentRegistry cr, SearchIntegrator searchFactory) {
      ClassLoader classLoader = cr.getGlobalComponentRegistry().getComponent(ClassLoader.class);
      ReflectionMatcher reflectionMatcher;
      if (searchFactory == null) {
         reflectionMatcher = new ReflectionMatcher(classLoader);
      } else {
         ReflectionEntityNamesResolver entityNamesResolver = new ReflectionEntityNamesResolver(classLoader);
         reflectionMatcher = new ReflectionMatcher(new HibernateSearchPropertyHelper(searchFactory, entityNamesResolver));
      }
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

   private void createQueryInterceptorIfNeeded(ComponentRegistry cr, Configuration cfg, SearchIntegrator searchFactory) {
      QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
      if (queryInterceptor == null) {
         queryInterceptor = buildQueryInterceptor(cfg, searchFactory, cr.getComponent(Cache.class));

         // Interceptor registration not needed, core configuration handling
         // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!
         AsyncInterceptorChain ic = cr.getComponent(AsyncInterceptorChain.class);

         ConfigurationBuilder builder = new ConfigurationBuilder().read(cfg);
         InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
         interceptorBuilder.interceptor(queryInterceptor);

         boolean txVersioned = Configurations.isTxVersioned(cfg);
         boolean isTotalOrder = cfg.transaction().transactionProtocol().isTotalOrder();

         Class<? extends DDAsyncInterceptor> wrappingInterceptor = EntryWrappingInterceptor.class;

         if (txVersioned) {
            wrappingInterceptor = isTotalOrder ?
                  TotalOrderVersionedEntryWrappingInterceptor.class : VersionedEntryWrappingInterceptor.class;
         }

         if (ic != null) ic.addInterceptorAfter(queryInterceptor, wrappingInterceptor);
         interceptorBuilder.after(wrappingInterceptor);

         if (ic != null) {
            cr.registerComponent(queryInterceptor, QueryInterceptor.class);
            cr.registerComponent(queryInterceptor, queryInterceptor.getClass().getName(), true);
         }
         cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
      }
   }

   private QueryInterceptor buildQueryInterceptor(Configuration cfg, SearchIntegrator searchFactory, Cache cache) {
      IndexModificationStrategy indexingStrategy = IndexModificationStrategy.configuredStrategy(searchFactory, cfg);
      return new QueryInterceptor(searchFactory, indexingStrategy, cache);
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

      // Register query mbeans
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

   private void registerQueryMBeans(ComponentRegistry cr, Configuration cfg, SearchIntegrator sf) {
      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      // Resolve MBean server instance
      GlobalConfiguration globalCfg = cr.getGlobalComponentRegistry().getGlobalConfiguration();
      mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);

      // Resolve jmx domain to use for query mbeans
      String cacheManagerName = globalCfg.globalJmxStatistics().cacheManagerName();
      String queryGroupName = getQueryGroupName(cacheManagerName, cache.getName());
      jmxDomain = JmxUtil.buildJmxDomain(globalCfg, mbeanServer, queryGroupName);

      // Register statistics MBean, but only enable if Infinispan config says so
      InfinispanQueryStatisticsInfo stats = new InfinispanQueryStatisticsInfo(sf);
      stats.setStatisticsEnabled(cfg.jmxStatistics().enabled());
      try {
         ObjectName statsObjName = new ObjectName(
               jmxDomain + ":" + queryGroupName + ",component=Statistics");
         JmxUtil.registerMBean(stats, statsObjName, mbeanServer);
      } catch (Exception e) {
         throw new CacheException(
               "Unable to register query module statistics mbean", e);
      }

      // Register mass indexer MBean, picking metadata from repo
      ManageableComponentMetadata massIndexerCompMetadata = cr.getGlobalComponentRegistry().getComponentMetadataRepo()
            .findComponentMetadata(MassIndexer.class)
            .toManageableComponentMetadata();
      try {
         // TODO: MassIndexer should be some kind of query cache component?
         DistributedExecutorMassIndexer massIndexer = new DistributedExecutorMassIndexer(cache, sf);
         ResourceDMBean mbean = new ResourceDMBean(massIndexer, massIndexerCompMetadata);
         ObjectName massIndexerObjName = new ObjectName(jmxDomain + ":"
               + queryGroupName + ",component=" + massIndexerCompMetadata.getJmxObjectName());
         JmxUtil.registerMBean(mbean, massIndexerObjName, mbeanServer);
      } catch (Exception e) {
         throw new CacheException("Unable to create ", e);
      }
   }

   private String getQueryGroupName(String cacheManagerName, String cacheName) {
      return "type=Query,manager=" + ObjectName.quote(cacheManagerName) + ",cache=" + ObjectName.quote(cacheName);
   }

   private boolean verifyChainContainsQueryInterceptor(ComponentRegistry cr) {
      AsyncInterceptorChain interceptorChain = cr.getComponent(AsyncInterceptorChain.class);
      return interceptorChain != null && interceptorChain.containsInterceptorType(QueryInterceptor.class, true);
   }

   private SearchIntegrator getSearchFactory(String cacheName, IndexingConfiguration indexingConfiguration, ComponentRegistry cr) {
      Object component = cr.getComponent(SearchIntegrator.class);
      SearchIntegrator searchFactory = null;
      if (component instanceof SearchIntegrator) { //could be the placeholder Object REMOVED_REGISTRY_COMPONENT
         searchFactory = (SearchIntegrator) component;
      }
      //defend against multiple initialization:
      if (searchFactory == null) {
         GlobalComponentRegistry globalComponentRegistry = cr.getGlobalComponentRegistry();
         EmbeddedCacheManager uninitializedCacheManager = globalComponentRegistry.getComponent(EmbeddedCacheManager.class);
         Properties indexingProperties = addProgrammaticMappings(indexingConfiguration.properties(), cr);
         Class<?>[] indexedEntities = indexingConfiguration.indexedEntities().toArray(new Class<?>[indexingConfiguration.indexedEntities().size()]);
         if (indexedEntities.length > 0 && hasInfinispanDirectory(indexingProperties)) {
            String metadataCacheName = getMetadataCacheName(indexingProperties);
            String lockingCacheName = getLockingCacheName(indexingProperties);
            String dataCacheName = getDataCacheName(indexingProperties);
            if (cacheName.equals(dataCacheName) || cacheName.equals(metadataCacheName) || cacheName.equals(lockingCacheName)) {
               // Infinispan Directory causes runtime circular dependencies so we need to postpone creation of indexes until all components are initialised
               indexedEntities = new Class[0];
            }
         }
         allowDynamicSortingByDefault(indexingProperties);
         // Set up the search factory for Hibernate Search first.
         SearchConfiguration config = new SearchableCacheConfiguration(indexedEntities, indexingProperties, uninitializedCacheManager, cr);
         searchFactory = new SearchIntegratorBuilder().configuration(config).buildSearchIntegrator();
         cr.registerComponent(searchFactory, SearchIntegrator.class);
      }
      return searchFactory;
   }

   /**
    * Dynamic index uninverting is deprecated: using it will cause warnings to be logged,
    * to encourage people to use the annotation org.hibernate.search.annotations.SortableField.
    * The default in Hibernate Search is to throw an exception rather than logging a warning;
    * we opt to be more lenient by default in the Infinispan use case, matching the behaviour
    * of previous versions of Hibernate Search.
    *
    * @param indexingProperties
    */
   private void allowDynamicSortingByDefault(Properties indexingProperties) {
      indexingProperties.putIfAbsent(Environment.INDEX_UNINVERTING_ALLOWED, Boolean.TRUE.toString());
   }

   private Properties addProgrammaticMappings(Properties indexingProperties, ComponentRegistry cr) {
      Iterator<ProgrammaticSearchMappingProvider> providers = ServiceFinder.load(ProgrammaticSearchMappingProvider.class).iterator();
      if (providers.hasNext()) {
         SearchMapping mapping = (SearchMapping) indexingProperties.get(Environment.MODEL_MAPPING);
         if (mapping == null) {
            mapping = new SearchMapping();
            Properties amendedProperties = new Properties();
            amendedProperties.putAll(indexingProperties);
            amendedProperties.put(Environment.MODEL_MAPPING, mapping);
            indexingProperties = amendedProperties;
         }
         Cache cache = cr.getComponent(Cache.class);
         while (providers.hasNext()) {
            ProgrammaticSearchMappingProvider provider = providers.next();
            if (log.isDebugEnabled()) {
               log.debugf("Loading programmatic search mappings for cache %s from provider : %s", cache.getName(), provider.getClass().getName());
            }
            provider.defineMappings(cache, mapping);
         }
      }
      return indexingProperties;
   }

   @Override
   public void cacheStopping(ComponentRegistry cr, String cacheName) {
      final QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
      if (queryInterceptor != null) {
         queryInterceptor.prepareForStopping();
      }
      //TODO move this to cacheStopped event (won't work right now as the ComponentRegistry is half empty at that point: ISPN-1006)
      Object searchFactoryIntegrator = cr.getComponent(SearchIntegrator.class);
      if (searchFactoryIntegrator != null && searchFactoryIntegrator != REMOVED_REGISTRY_COMPONENT) {
         ((SearchIntegrator) searchFactoryIntegrator).close();
         //free some memory by de-registering the SearchFactory
         cr.registerComponent(REMOVED_REGISTRY_COMPONENT, SearchIntegrator.class);
      }

      // Unregister MBeans
      if (mbeanServer != null) {
         String cacheManagerName = cr.getGlobalComponentRegistry().getGlobalConfiguration().globalJmxStatistics().cacheManagerName();
         String queryMBeanFilter = jmxDomain + ":" + getQueryGroupName(cacheManagerName, cacheName) + ",*";
         JmxUtil.unregisterMBeans(queryMBeanFilter, mbeanServer);
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
      QueryCache queryCache = new QueryCache();
      gcr.registerComponent(queryCache, QueryCache.class);

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
      externalizerMap.put(ExternalizerIds.CLUSTERED_QUERY_TOPDOCS, new ClusteredTopDocsExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_TOPDOCS, new LuceneTopDocsExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_FIELD_SCORE_DOC, new LuceneFieldDocExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_SCORE_DOC, new LuceneScoreDocExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_TOPFIELDDOCS, new LuceneTopFieldDocsExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_MATCH_ALL, new LuceneMatchAllQueryExternalizer());
      externalizerMap.put(ExternalizerIds.INDEX_WORKER, new IndexWorker.Externalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_BYTES_REF, new LuceneBytesRefExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_PREFIX, new LucenePrefixQueryExternalizer());
      externalizerMap.put(ExternalizerIds.LUCENE_QUERY_WILDCARD, new LuceneWildcardQueryExternalizer());
   }

}
