package org.infinispan.query.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.hibernate.search.cfg.Environment;
import org.hibernate.search.cfg.SearchMapping;
import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.spi.SearchFactoryBuilder;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.IndexModificationStrategy;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.backend.SearchableCacheConfiguration;
import org.infinispan.query.clustered.QueryBox;
import org.infinispan.query.dsl.embedded.impl.JPACacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.impl.externalizers.ClusteredTopDocsExternalizer;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.query.impl.externalizers.LuceneBooleanQueryExternalizer;
import org.infinispan.query.impl.externalizers.LuceneFieldDocExternalizer;
import org.infinispan.query.impl.externalizers.LuceneMatchAllQueryExternalizer;
import org.infinispan.query.impl.externalizers.LuceneScoreDocExternalizer;
import org.infinispan.query.impl.externalizers.LuceneSortExternalizer;
import org.infinispan.query.impl.externalizers.LuceneSortFieldExternalizer;
import org.infinispan.query.impl.externalizers.LuceneTermExternalizer;
import org.infinispan.query.impl.externalizers.LuceneTermQueryExternalizer;
import org.infinispan.query.impl.externalizers.LuceneTopDocsExternalizer;
import org.infinispan.query.impl.externalizers.LuceneTopFieldDocsExternalizer;
import org.infinispan.query.impl.massindex.DistributedExecutorMassIndexer;
import org.infinispan.query.impl.massindex.IndexWorker;
import org.infinispan.query.logging.Log;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.infinispan.registry.impl.ClusterRegistryImpl;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

import java.util.Set;

import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.infinispan.query.impl.IndexPropertyInspector.*;

/**
 * Lifecycle of the Query module: initializes the Hibernate Search engine and shuts it down
 * at cache stop.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
@MetaInfServices(org.infinispan.lifecycle.ModuleLifecycle.class)
public class LifecycleManager extends AbstractModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   private static final Object REMOVED_REGISTRY_COMPONENT = new Object();

   private MBeanServer mbeanServer;

   private String jmxDomain;

   private static final Set<String> DEFAULT_CACHES = CollectionFactory.makeSet(
         DEFAULT_LOCKING_CACHENAME,
         DEFAULT_INDEXESDATA_CACHENAME,
         DEFAULT_INDEXESMETADATA_CACHENAME
   );

   /**
    * Registers the Search interceptor in the cache before it gets started
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      cr.registerComponent(new ReflectionMatcher(null), ReflectionMatcher.class);

      if (cfg.indexing().index().isEnabled()) {
         log.registeringQueryInterceptor();
         SearchIntegrator searchFactory = getSearchFactory(cfg.indexing().properties(), cr);
         createQueryInterceptorIfNeeded(cr, cfg, searchFactory);
         EmbeddedCacheManager cacheManager = cr.getGlobalComponentRegistry().getComponent(EmbeddedCacheManager.class);
         addCacheDependencyIfNeeded(cacheName, cacheManager, cfg.indexing().properties());
      }
   }

   private void addCacheDependencyIfNeeded(String cacheStarting, EmbeddedCacheManager cacheManager, Properties properties) {
      if (!ClusterRegistryImpl.GLOBAL_REGISTRY_CACHE_NAME.equals(cacheStarting)) {
         cacheManager.addCacheDependency(cacheStarting, ClusterRegistryImpl.GLOBAL_REGISTRY_CACHE_NAME);
      }
      if (hasInfinispanDirectory(properties) && !DEFAULT_CACHES.contains(cacheStarting)) {
         String metadataCacheName = getMetadataCacheName(properties);
         String lockingCacheName = getLockingCacheName(properties);
         String dataCacheName = getDataCacheName(properties);
         if (!metadataCacheName.equals(cacheStarting)) {
            cacheManager.addCacheDependency(cacheStarting, metadataCacheName);
         }
         if (!lockingCacheName.equals(cacheStarting)) {
            cacheManager.addCacheDependency(cacheStarting, lockingCacheName);
         }
         if (!dataCacheName.equals(cacheStarting)) {
            cacheManager.addCacheDependency(cacheStarting, dataCacheName);
         }
      }
   }

   private void createQueryInterceptorIfNeeded(ComponentRegistry cr, Configuration cfg, SearchIntegrator searchFactory) {
      QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
      if (queryInterceptor == null) {
         queryInterceptor = buildQueryInterceptor(cfg, searchFactory);

         // Interceptor registration not needed, core configuration handling
         // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!
         InterceptorChain ic = cr.getComponent(InterceptorChain.class);

         ConfigurationBuilder builder = new ConfigurationBuilder().read(cfg);
         InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
         interceptorBuilder.interceptor(queryInterceptor);

         if (!cfg.transaction().transactionMode().isTransactional()) {
            if (ic != null) ic.addInterceptorAfter(queryInterceptor, NonTransactionalLockingInterceptor.class);
            interceptorBuilder.after(NonTransactionalLockingInterceptor.class);
         } else if (cfg.transaction().lockingMode() == LockingMode.OPTIMISTIC) {
            if (ic != null) ic.addInterceptorAfter(queryInterceptor, OptimisticLockingInterceptor.class);
            interceptorBuilder.after(OptimisticLockingInterceptor.class);
         } else {
            if (ic != null) ic.addInterceptorAfter(queryInterceptor, PessimisticLockingInterceptor.class);
            interceptorBuilder.after(PessimisticLockingInterceptor.class);
         }
         if (ic != null) {
            cr.registerComponent(queryInterceptor, QueryInterceptor.class);
            cr.registerComponent(queryInterceptor, queryInterceptor.getClass().getName(), true);
         }
         cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
      }
   }

   private QueryInterceptor buildQueryInterceptor(Configuration cfg, SearchIntegrator searchFactory) {
      IndexModificationStrategy indexingStrategy = IndexModificationStrategy.configuredStrategy(searchFactory, cfg);
      return new QueryInterceptor(searchFactory, indexingStrategy);
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      boolean indexingEnabled = configuration.indexing().index().isEnabled();
      if ( ! indexingEnabled ) {
         if ( verifyChainContainsQueryInterceptor(cr) ) {
            throw new IllegalStateException( "It was NOT expected to find the Query interceptor registered in the InterceptorChain as indexing was disabled, but it was found" );
         }
         return;
      }
      if ( ! verifyChainContainsQueryInterceptor(cr) ) {
         throw new IllegalStateException( "It was expected to find the Query interceptor registered in the InterceptorChain but it wasn't found" );
      }

      // initializing the query module command initializer.
      // we can t inject Cache and CacheManager with @inject in there
      Cache<?, ?> cache = cr.getComponent(Cache.class);
      CommandInitializer initializer = cr.getComponent(CommandInitializer.class);
      EmbeddedCacheManager cacheManager = cr.getGlobalComponentRegistry().getComponent(EmbeddedCacheManager.class);
      initializer.setCacheManager(cacheManager);

      QueryBox queryBox = new QueryBox();
      queryBox.setCache(cache.getAdvancedCache());
      cr.registerComponent(queryBox, QueryBox.class);

      // Register query mbeans
      registerQueryMBeans(cache.getAdvancedCache(), cr, cacheName);
   }

   private void registerQueryMBeans(AdvancedCache cache,
         ComponentRegistry cr, String cacheName) {
      Configuration cfg = cache.getCacheConfiguration();
      SearchIntegrator sf = getSearchFactory(
            cfg.indexing().properties(), cr);

      // Resolve MBean server instance
      GlobalConfiguration globalCfg =
            cr.getGlobalComponentRegistry().getGlobalConfiguration();
      mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);

      // Resolve jmx domain to use for query mbeans
      String cacheManagerName = cr.getGlobalComponentRegistry().getGlobalConfiguration().globalJmxStatistics().cacheManagerName();
      String queryGroupName = getQueryGroupName(cacheManagerName, cacheName);
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
         DistributedExecutorMassIndexer maxIndexer = new DistributedExecutorMassIndexer(cache, sf);
         ResourceDMBean mbean = new ResourceDMBean(maxIndexer, massIndexerCompMetadata);
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
      InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
      return interceptorChain.containsInterceptorType(QueryInterceptor.class, true);
   }

   private SearchIntegrator getSearchFactory(Properties indexingProperties, ComponentRegistry cr) {
      Object component = cr.getComponent(SearchIntegrator.class);
      SearchIntegrator searchFactory = null;
      if (component instanceof SearchIntegrator) { //could be the placeholder Object REMOVED_REGISTRY_COMPONENT
         searchFactory = (SearchIntegrator) component;
      }
      //defend against multiple initialization:
      if (searchFactory==null) {
         GlobalComponentRegistry globalComponentRegistry = cr.getGlobalComponentRegistry();
         EmbeddedCacheManager uninitializedCacheManager = globalComponentRegistry.getComponent(EmbeddedCacheManager.class);
         indexingProperties = addProgrammaticMappings(indexingProperties, cr);
         // Set up the search factory for Hibernate Search first.
         SearchConfiguration config = new SearchableCacheConfiguration(new Class[0], indexingProperties, uninitializedCacheManager, cr);
         searchFactory = new SearchFactoryBuilder().configuration(config).buildSearchFactory();
         cr.registerComponent(searchFactory, SearchIntegrator.class);
      }
      return searchFactory;
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
         if (!(interceptorConfig.interceptor() instanceof QueryInterceptor)) {
            customInterceptorsBuilder.addInterceptor().read(interceptorConfig);
         }
      }

      cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      QueryCache queryCache = new QueryCache();
      gcr.registerComponent(queryCache, QueryCache.class);

      Map<Integer,AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.JPA_FILTER_AND_CONVERTER, new JPAFilterAndConverter.JPAFilterAndConverterExternalizer());
      externalizerMap.put(ExternalizerIds.JPA_FILTER_RESULT, new JPAFilterAndConverter.FilterResultExternalizer());
      externalizerMap.put(ExternalizerIds.JPA_CACHE_EVENT_FILTER_CONVERTER, new JPACacheEventFilterConverter.Externalizer());
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
   }

}
