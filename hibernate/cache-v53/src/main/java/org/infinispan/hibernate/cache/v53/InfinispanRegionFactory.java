/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v53;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.CacheTransactionSynchronization;
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.StandardCacheTransactionSynchronization;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.support.RegionNameQualifier;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.service.ServiceRegistry;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.hibernate.cache.commons.DataType;
import org.infinispan.hibernate.cache.commons.DefaultCacheManagerProvider;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.hibernate.cache.commons.TimeSource;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.commons.util.CacheCommandFactory;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.v53.impl.DomainDataRegionImpl;
import org.infinispan.hibernate.cache.v53.impl.QueryResultsRegionImpl;
import org.infinispan.hibernate.cache.v53.impl.ClusteredTimestampsRegionImpl;
import org.infinispan.hibernate.cache.v53.impl.TimestampsRegionImpl;
import org.infinispan.hibernate.cache.spi.EmbeddedCacheManagerProvider;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.TransactionMode;

/**
 * A {@link RegionFactory} for <a href="http://www.jboss.org/infinispan">Infinispan</a>-backed cache
 * regions.
 *
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
public class InfinispanRegionFactory implements RegionFactory, TimeSource, InfinispanProperties {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( InfinispanRegionFactory.class );

   /**
    * Defines custom mapping for regionName -> cacheName and also DataType.key -> cacheName
    * (for the case that you want to change the cache configuration for whole type)
    */
   protected final Map<String, String> baseConfigurations = new HashMap<>();
   /**
    * Defines configuration properties applied on top of configuration set in any file, by regionName or DataType.key
    */
   protected final Map<String, ConfigurationBuilder> configOverrides = new HashMap<>();

   private CacheKeysFactory cacheKeysFactory;
   private final Map<DataType, Configuration> dataTypeConfigurations = new HashMap<>();
   private EmbeddedCacheManager manager;

   private List<InfinispanBaseRegion> regions = new ArrayList<>();
   private SessionFactoryOptions settings;

   private Boolean globalStats;

   /**
    * Create a new instance using the default configuration.
    */
   public InfinispanRegionFactory() {
   }

   /**
    * Create a new instance using conifguration properties in <code>props</code>.
    *
    * @param props Environmental properties; currently unused.
    */
   @SuppressWarnings("UnusedParameters")
   public InfinispanRegionFactory(Properties props) {
      this();
   }

   @Override
   public DomainDataRegion buildDomainDataRegion(DomainDataRegionConfig regionConfig, DomainDataRegionBuildingContext buildingContext) {
      log.debugf("Building domain data region [%s] entities=%s collections=%s naturalIds=%s",
            regionConfig.getRegionName(),
            regionConfig.getEntityCaching(),
            regionConfig.getCollectionCaching(),
            regionConfig.getNaturalIdCaching()
      );
      // TODO: data type is probably deprecated, but we need it for backwards-compatible configuration
      DataType dataType;
      int entities = regionConfig.getEntityCaching().size();
      int collections = regionConfig.getCollectionCaching().size();
      int naturalIds = regionConfig.getNaturalIdCaching().size();
      if (entities > 0 && collections == 0 && naturalIds == 0) {
         dataType = regionConfig.getEntityCaching().stream()
               .allMatch(c -> !c.isMutable()) ? DataType.IMMUTABLE_ENTITY : DataType.ENTITY;
      } else if (entities == 0 && collections > 0 && naturalIds == 0) {
         dataType = DataType.COLLECTION;
      } else if (entities == 0 && collections == 0 && naturalIds > 0) {
         dataType = DataType.NATURAL_ID;
      } else {
         // some mix, let's use entity
         dataType = DataType.ENTITY;
      }

      AdvancedCache cache = getCache(qualify(regionConfig.getRegionName()), dataType);
      DomainDataRegionImpl region = new DomainDataRegionImpl(cache, regionConfig, this, getCacheKeysFactory());
      startRegion(region);
      return region;
   }

   @Override
   public QueryResultsRegion buildQueryResultsRegion(String regionName, SessionFactoryImplementor sessionFactory) {
      log.debugf("Building query results cache region [%s]", regionName);

      AdvancedCache cache = getCache(qualify(regionName), DataType.QUERY);
      QueryResultsRegionImpl region = new QueryResultsRegionImpl(cache, regionName, this);
      startRegion(region);
      return region;
   }

   @Override
   public TimestampsRegion buildTimestampsRegion(String regionName, SessionFactoryImplementor sessionFactory) {
      log.debugf("Building timestamps cache region [%s]", regionName);

      final AdvancedCache cache = getCache(qualify(regionName), DataType.TIMESTAMPS);
      TimestampsRegionImpl region = createTimestampsRegion(cache, regionName);
      startRegion(region);
      return region;
   }

   protected TimestampsRegionImpl createTimestampsRegion(
         AdvancedCache cache, String regionName) {
      if ( Caches.isClustered(cache) ) {
         return new ClusteredTimestampsRegionImpl(cache, regionName, this);
      } else {
         return new TimestampsRegionImpl(cache, regionName, this);
      }
   }

   public Configuration getPendingPutsCacheConfiguration() {
      return dataTypeConfigurations.get(DataType.PENDING_PUTS);
   }

   protected CacheKeysFactory getCacheKeysFactory() {
      return cacheKeysFactory;
   }

   @Override
   public boolean isMinimalPutsEnabledByDefault() {
      return false;
   }

   @Override
   public AccessType getDefaultAccessType() {
      return AccessType.TRANSACTIONAL;
   }

   @Override
   public String qualify(String regionName) {
      return RegionNameQualifier.INSTANCE.qualify(regionName, settings);
   }

   @Override
   public CacheTransactionSynchronization createTransactionContext(SharedSessionContractImplementor session) {
      // TODO: we will use another synchronization soon...
      return new StandardCacheTransactionSynchronization(this);
   }

   @Override
   public long nextTimestamp() {
      return System.currentTimeMillis();
   }

   public void setCacheManager(EmbeddedCacheManager manager) {
      this.manager = manager;
   }

   public EmbeddedCacheManager getCacheManager() {
      return manager;
   }

   @Override
   public void start(SessionFactoryOptions settings, Map configValues) throws CacheException {
      log.debug( "Starting Infinispan region factory" );

      // determine the CacheKeysFactory to use...
      this.cacheKeysFactory = determineCacheKeysFactory( settings, configValues );

      try {
         this.settings = settings;

         for (Object k : configValues.keySet()) {
            final String key = (String) k;
            int prefixLoc;
            if ((prefixLoc = key.indexOf( PREFIX )) != -1) {
               parseProperty( prefixLoc, key, extractProperty(key, configValues));
            }
         }

         String globalStatsStr = extractProperty(INFINISPAN_GLOBAL_STATISTICS_PROP, configValues);
         if ( globalStatsStr != null ) {
            globalStats = Boolean.parseBoolean(globalStatsStr);
         }
         if (configValues.containsKey(INFINISPAN_USE_SYNCHRONIZATION_PROP)) {
            log.propertyUseSynchronizationDeprecated();
         }

         StandardServiceRegistry serviceRegistry = settings.getServiceRegistry();
         manager = createCacheManager(toProperties(configValues), serviceRegistry);
         defineDataTypeCacheConfigurations(serviceRegistry);
      }
      catch (CacheException ce) {
         throw ce;
      }
      catch (Throwable t) {
         throw log.unableToStart(t);
      }
   }

   private Properties toProperties(Map<Object, Object> configValues) {
      Properties properties = new Properties();
      for (Map.Entry<Object, Object> entry : configValues.entrySet()) {
         properties.put(entry.getKey(), entry.getValue());
      }
      return properties;
   }

   private CacheKeysFactory determineCacheKeysFactory(SessionFactoryOptions settings, Map properties) {
      return settings.getServiceRegistry().getService( StrategySelector.class ).resolveDefaultableStrategy(
            CacheKeysFactory.class,
            properties.get( AvailableSettings.CACHE_KEYS_FACTORY ),
            DefaultCacheKeysFactory.INSTANCE
      );
   }

   /* This method is overridden in WildFly, so the signature must not change. */
   protected EmbeddedCacheManager createCacheManager(Properties properties, ServiceRegistry serviceRegistry) {
      for (EmbeddedCacheManagerProvider provider : ServiceLoader.load(EmbeddedCacheManagerProvider.class, EmbeddedCacheManagerProvider.class.getClassLoader())) {
         EmbeddedCacheManager cacheManager = provider.getEmbeddedCacheManager(properties);
         if (cacheManager != null) {
            return cacheManager;
         }
      }
      return new DefaultCacheManagerProvider(serviceRegistry).getEmbeddedCacheManager(properties);
   }

   @Override
   public void stop() {
      log.debug( "Stop region factory" );
      stopCacheRegions();
      stopCacheManager();
   }

   protected void stopCacheRegions() {
      log.debug( "Clear region references" );
      getCacheCommandFactory().clearRegions(regions);
      // Ensure we cleanup any caches we created
      regions.forEach( region -> {
         region.destroy();
         manager.undefineConfiguration( region.getCache().getName() );
      } );
      regions.clear();
   }

   protected void stopCacheManager() {
      log.debug( "Stop cache manager" );
      manager.stop();
   }

   private ConfigurationBuilderHolder loadConfiguration(ServiceRegistry serviceRegistry, String configFile) {
      final FileLookup fileLookup = FileLookupFactory.newInstance();
      final ClassLoader infinispanClassLoader = InfinispanRegionFactory.class.getClassLoader();
      return serviceRegistry.getService( ClassLoaderService.class ).workWithClassLoader(
            classLoader -> {
               InputStream is = null;
               try {
                  is = fileLookup.lookupFile(configFile, classLoader );
                  if ( is == null ) {
                     // when it's not a user-provided configuration file, it might be a default configuration file,
                     // and if that's included in [this] module might not be visible to the ClassLoaderService:
                     classLoader = infinispanClassLoader;
                     // This time use lookupFile*Strict* so to provide an exception if we can't find it yet:
                     is = FileLookupFactory.newInstance().lookupFileStrict(configFile, classLoader );
                  }
                  final ParserRegistry parserRegistry = new ParserRegistry( infinispanClassLoader );
                  final ConfigurationBuilderHolder holder = parseWithOverridenClassLoader( parserRegistry, is, infinispanClassLoader );

                  return holder;
               }
               catch (IOException e) {
                  throw log.unableToCreateCacheManager(e);
               }
               finally {
                  Util.close( is );
               }
            }
      );
   }

   private static ConfigurationBuilderHolder parseWithOverridenClassLoader(ParserRegistry configurationParser, InputStream is, ClassLoader infinispanClassLoader) {
      // Infinispan requires the context ClassLoader to have full visibility on all
      // its components and eventual extension points even *during* configuration parsing.
      final Thread currentThread = Thread.currentThread();
      final ClassLoader originalContextClassLoader = currentThread.getContextClassLoader();
      try {
         currentThread.setContextClassLoader( infinispanClassLoader );
         ConfigurationBuilderHolder builderHolder = configurationParser.parse( is );
         // Workaround Infinispan's ClassLoader strategies to bend to our will:
         builderHolder.getGlobalConfigurationBuilder().classLoader( infinispanClassLoader );
         return builderHolder;
      } finally {
         currentThread.setContextClassLoader( originalContextClassLoader );
      }
   }

   private void startRegion(InfinispanBaseRegion region) {
      regions.add(region);
      getCacheCommandFactory().addRegion(region);
   }

   private void parseProperty(int prefixLoc, String key, String value) {
      final ConfigurationBuilder builder;
      int suffixLoc;
      if ( (suffixLoc = key.indexOf( CONFIG_SUFFIX )) != -1 && !key.equals( INFINISPAN_CONFIG_RESOURCE_PROP )) {
         String regionName = key.substring( prefixLoc + PREFIX.length(), suffixLoc );
         baseConfigurations.put(regionName, value);
      } else if (key.contains(DEPRECATED_STRATEGY_SUFFIX)) {
         log.ignoringDeprecatedProperty(DEPRECATED_STRATEGY_SUFFIX);
      } else if ( (suffixLoc = key.indexOf( WAKE_UP_INTERVAL_SUFFIX )) != -1
            || (suffixLoc = key.indexOf(DEPRECATED_WAKE_UP_INTERVAL_SUFFIX)) != -1 ) {
         builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
         builder.expiration().wakeUpInterval( Long.parseLong(value) );
      } else if ( (suffixLoc = key.indexOf(SIZE_SUFFIX)) != -1 ) {
         builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
         builder.memory().size( Long.parseLong(value) );
      } else if ( (suffixLoc = key.indexOf(DEPRECATED_MAX_ENTRIES_SUFFIX)) != -1 ) {
         log.deprecatedProperty(DEPRECATED_MAX_ENTRIES_SUFFIX, SIZE_SUFFIX);
         builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
         builder.memory().size( Long.parseLong(value) );
      } else if ( (suffixLoc = key.indexOf( LIFESPAN_SUFFIX )) != -1 ) {
         builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
         builder.expiration().lifespan( Long.parseLong(value) );
      } else if ( (suffixLoc = key.indexOf( MAX_IDLE_SUFFIX )) != -1 ) {
         builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
         builder.expiration().maxIdle( Long.parseLong(value) );
      }
   }

   private String extractProperty(String key, Map properties) {
      final String value = ConfigurationHelper.extractPropertyValue( key, properties );
      log.debugf( "Configuration override via property %s: %s", key, value );
      return value;
   }

   private ConfigurationBuilder getOrCreateConfig(int prefixLoc, String key, int suffixLoc) {
      final String name = key.substring( prefixLoc + PREFIX.length(), suffixLoc );
      return configOverrides.computeIfAbsent(name, Void -> new ConfigurationBuilder());
   }

   private void defineDataTypeCacheConfigurations(ServiceRegistry serviceRegistry) {
      String defaultResource = manager.getCacheManagerConfiguration().isClustered() ? DEF_INFINISPAN_CONFIG_RESOURCE : INFINISPAN_CONFIG_LOCAL_RESOURCE;
      ConfigurationBuilderHolder defaultConfiguration = DefaultCacheManagerProvider.loadConfiguration(serviceRegistry, defaultResource);
      for ( DataType type : DataType.values() ) {
         String cacheName = baseConfigurations.get(type.key);
         if (cacheName == null) {
            cacheName = type.defaultCacheName;
         }
         Configuration configuration = manager.getCacheConfiguration(cacheName);
         ConfigurationBuilder builder;
         if (configuration == null) {
            log.debugf("Cache configuration not found for %s", type);
            if (!cacheName.equals(type.defaultCacheName)) {
               log.customConfigForTypeNotFound(cacheName, type.key);
            }
            builder = defaultConfiguration.getNamedConfigurationBuilders().get(type.defaultCacheName);
            if (builder == null) {
               throw new IllegalStateException("Generic data types must have default configuration, none found for " + type);
            }
         } else {
            builder = new ConfigurationBuilder().read(configuration);
         }
         ConfigurationBuilder override = configOverrides.get( type.key );
         if (override != null) {
            builder.read(override.build(false));
         }
         builder.template(true);
         unsetTransactions(builder);
         dataTypeConfigurations.put(type, builder.build());
      }
   }

   protected AdvancedCache getCache(String cacheName, DataType type) {
      if (!manager.cacheExists(cacheName)) {
         String templateCacheName = baseConfigurations.get(cacheName);
         Configuration configuration = null;
         ConfigurationBuilder builder = new ConfigurationBuilder();
         if (templateCacheName != null) {
            configuration = manager.getCacheConfiguration(templateCacheName);
            if (configuration == null) {
               log.customConfigForRegionNotFound(templateCacheName, cacheName, type.key);
            } else {
               log.debugf("Region '%s' will use cache template '%s'", cacheName, templateCacheName);
               builder.read(configuration);
               unsetTransactions(builder);
               // do not apply data type overrides to regions that set special cache configuration
            }
         }
         if (configuration == null) {
            configuration = dataTypeConfigurations.get(type);
            if (configuration == null) {
               throw new IllegalStateException("Configuration not defined for type " + type.key);
            }
            builder.read(configuration);
            // overrides for data types are already applied, but we should check custom ones
         }
         ConfigurationBuilder override = configOverrides.get(cacheName);
         if (override != null) {
            log.debugf("Region '%s' has additional configuration set through properties.", cacheName);
            builder.read(override.build(false));
         }
         if (globalStats != null) {
            builder.jmxStatistics().enabled(globalStats).available(globalStats);
         }
         configuration = builder.build();
         type.validate(configuration);
         manager.defineConfiguration(cacheName, configuration);
      }
      final AdvancedCache cache = manager.getCache( cacheName ).getAdvancedCache();
      // TODO: not sure if this is needed in recent Infinispan
      if ( !cache.getStatus().allowInvocations() ) {
         cache.start();
      }
      return cache;
   }

   private void unsetTransactions(ConfigurationBuilder builder) {
      if (builder.transaction().transactionMode().isTransactional()) {
         log.transactionalConfigurationIgnored();
         builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL).transactionManagerLookup(null);
      }
   }


   private CacheCommandFactory getCacheCommandFactory() {
      final GlobalComponentRegistry globalCr = manager.getGlobalComponentRegistry();

      final Map<Byte, ModuleCommandFactory> factories =
            globalCr.getComponent( "org.infinispan.modules.command.factories" );

      for ( ModuleCommandFactory factory : factories.values() ) {
         if ( factory instanceof CacheCommandFactory ) {
            return (CacheCommandFactory) factory;
         }
      }

      throw log.cannotInstallCommandFactory();
   }
}
