package org.infinispan.query.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.TreeMap;

import org.hibernate.search.Environment;
import org.hibernate.search.cfg.SearchMapping;
import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.jmx.StatisticsInfo;
import org.hibernate.search.spi.SearchFactoryBuilder;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.hibernate.search.stat.Statistics;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.LocalQueryInterceptor;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.backend.SearchableCacheConfiguration;
import org.infinispan.query.clustered.QueryBox;
import org.infinispan.query.impl.massindex.MapReduceMassIndexer;
import org.infinispan.query.logging.Log;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.LogFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Lifecycle of the Query module: initializes the Hibernate Search engine and shuts it down
 * at cache stop.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class LifecycleManager extends AbstractModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   private final Map<String, SearchFactoryIntegrator> searchFactoriesToShutdown = new TreeMap<String,SearchFactoryIntegrator>();

   private static final Object REMOVED_REGISTRY_COMPONENT = new Object();

   private MBeanServer mbeanServer;     //todo [anistor] these should not be global! they are per cache manager

   private ComponentMetadataRepo metadataRepo;

   private String jmxDomain;

   @Override
   public void cacheManagerStarting(
         GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      metadataRepo = gcr.getComponentMetadataRepo();
   }

   /**
    * Registers the Search interceptor in the cache before it gets started
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      if (cfg.indexing().enabled()) {
         log.registeringQueryInterceptor();
         SearchFactoryIntegrator searchFactory = getSearchFactory(cfg.indexing().properties(), cr);
         createQueryInterceptorIfNeeded(cr, cfg, searchFactory);
      }
   }

   private void createQueryInterceptorIfNeeded(ComponentRegistry cr, Configuration cfg, SearchFactoryIntegrator searchFactory) {
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

   private QueryInterceptor buildQueryInterceptor(Configuration cfg, SearchFactoryIntegrator searchFactory) {
      if ( cfg.indexing().indexLocalOnly() ) {
         return new LocalQueryInterceptor(searchFactory);
      }
      else {
         return new QueryInterceptor(searchFactory);
      }
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      boolean indexingEnabled = configuration.indexing().enabled();
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
      initializer.setCache(cache, cacheManager);

      QueryBox queryBox = new QueryBox();
      queryBox.setCache(cache.getAdvancedCache());
      cr.registerComponent(queryBox, QueryBox.class);

      // Register query mbeans
      registerQueryMBeans(cache.getAdvancedCache(), cr, cacheName);
   }

   private void registerQueryMBeans(AdvancedCache cache,
         ComponentRegistry cr, String cacheName) {
      Configuration cfg = cache.getCacheConfiguration();
      SearchFactoryIntegrator sf = getSearchFactory(
            cfg.indexing().properties(), cr);

      // Resolve MBean server instance
      GlobalConfiguration globalCfg =
            cr.getGlobalComponentRegistry().getGlobalConfiguration();
      mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);

      // Resolve jmx domain to use for query mbeans
      String queryGroupName = getQueryGroupName(cacheName);
      jmxDomain = JmxUtil.buildJmxDomain(globalCfg, mbeanServer, queryGroupName);

      // Register statistics MBean, but only enable if Infinispan config says so
      Statistics stats = sf.getStatistics();
      stats.setStatisticsEnabled(cfg.jmxStatistics().enabled());
      try {
         ObjectName statsObjName = new ObjectName(
               jmxDomain + ":" + queryGroupName + ",component=Statistics");
         JmxUtil.registerMBean(new StatisticsInfo(stats), statsObjName, mbeanServer);
      } catch (Exception e) {
         throw new CacheException(
               "Unable to register query module statistics mbean", e);
      }

      // Register mass indexer MBean, picking metadata from repo
      ManageableComponentMetadata metadata = metadataRepo
            .findComponentMetadata(MassIndexer.class)
            .toManageableComponentMetadata();
      try {
         // TODO: MassIndexer should be some kind of query cache component?
         MapReduceMassIndexer maxIndexer = new MapReduceMassIndexer(cache, sf);
         ResourceDMBean mbean = new ResourceDMBean(maxIndexer, metadata);
         ObjectName massIndexerObjName = new ObjectName(jmxDomain + ":"
               + queryGroupName+ ",component=" + metadata.getJmxObjectName());
         JmxUtil.registerMBean(mbean, massIndexerObjName, mbeanServer);
      } catch (Exception e) {
         throw new CacheException("Unable to create ", e);
      }
   }

   private String getQueryGroupName(String cacheName) {
      return "type=Query,name=" + ObjectName.quote(cacheName);
   }

   private boolean verifyChainContainsQueryInterceptor(ComponentRegistry cr) {
      InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
      return interceptorChain.containsInterceptorType(QueryInterceptor.class, true);
   }

   private SearchFactoryIntegrator getSearchFactory(Properties indexingProperties, ComponentRegistry cr) {
      Object component = cr.getComponent(SearchFactoryIntegrator.class);
      SearchFactoryIntegrator searchFactory = null;
      if (component instanceof SearchFactoryIntegrator) { //could be the placeholder Object REMOVED_REGISTRY_COMPONENT
         searchFactory = (SearchFactoryIntegrator) component;
      }
      //defend against multiple initialization:
      if (searchFactory==null) {
         GlobalComponentRegistry globalComponentRegistry = cr.getGlobalComponentRegistry();
         EmbeddedCacheManager uninitializedCacheManager = globalComponentRegistry.getComponent(EmbeddedCacheManager.class);
         indexingProperties = addProgrammaticMappings(indexingProperties, cr);
         // Set up the search factory for Hibernate Search first.
         SearchConfiguration config = new SearchableCacheConfiguration(new Class[0], indexingProperties, uninitializedCacheManager, cr);
         searchFactory = new SearchFactoryBuilder().configuration(config).buildSearchFactory();
         cr.registerComponent(searchFactory, SearchFactoryIntegrator.class);
      }
      return searchFactory;
   }

   private Properties addProgrammaticMappings(Properties indexingProperties, ComponentRegistry cr) {
      Iterator<ProgrammaticSearchMappingProvider> providers = ServiceLoader.load(ProgrammaticSearchMappingProvider.class).iterator();
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
            provider.defineMappings(cache, mapping);
         }
      }
      return indexingProperties;
   }

   @Override
   public void cacheStopping(ComponentRegistry cr, String cacheName) {
      //TODO move this to cacheStopped event (won't work right now as the ComponentRegistry is half empty at that point: ISPN-1006)
      Object searchFactoryIntegrator = cr.getComponent(SearchFactoryIntegrator.class);
      if (searchFactoryIntegrator != null && searchFactoryIntegrator != REMOVED_REGISTRY_COMPONENT) {
         searchFactoriesToShutdown.put(cacheName, (SearchFactoryIntegrator) searchFactoryIntegrator);
         //free some memory by de-registering the SearchFactory
         cr.registerComponent(REMOVED_REGISTRY_COMPONENT, SearchFactoryIntegrator.class);
      }

      // Unregister MBeans
      if (mbeanServer != null) {
         String queryMBeanFilter = jmxDomain + ":" + getQueryGroupName(cacheName) + ",*";
         JmxUtil.unregisterMBeans(queryMBeanFilter, mbeanServer);
      }
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      SearchFactoryIntegrator searchFactoryIntegrator = searchFactoriesToShutdown.remove(cacheName);
      if (searchFactoryIntegrator != null) {
         searchFactoryIntegrator.close();
      }

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

}
