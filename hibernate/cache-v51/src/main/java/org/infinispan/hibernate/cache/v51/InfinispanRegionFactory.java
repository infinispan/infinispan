/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v51;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import javax.transaction.TransactionManager;

import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.service.ServiceRegistry;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.hibernate.cache.commons.DataType;
import org.infinispan.hibernate.cache.commons.DefaultCacheManagerProvider;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.hibernate.cache.commons.TimeSource;
import org.infinispan.hibernate.cache.v51.tm.HibernateTransactionManagerLookup;
import org.infinispan.hibernate.cache.commons.util.CacheCommandFactory;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.spi.EmbeddedCacheManagerProvider;
import org.infinispan.hibernate.cache.v51.collection.CollectionRegionImpl;
import org.infinispan.hibernate.cache.v51.entity.EntityRegionImpl;
import org.infinispan.hibernate.cache.v51.naturalid.NaturalIdRegionImpl;
import org.infinispan.hibernate.cache.v51.query.QueryResultsRegionImpl;
import org.infinispan.hibernate.cache.v51.timestamp.ClusteredTimestampsRegionImpl;
import org.infinispan.hibernate.cache.v51.timestamp.TimestampsRegionImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.hibernate.cache.v51.impl.BaseRegion;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;

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

	private org.infinispan.transaction.lookup.TransactionManagerLookup transactionManagerlookup;
	private TransactionManager transactionManager;

	private List<BaseRegion> regions = new ArrayList<>();
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
	public CollectionRegion buildCollectionRegion(String regionName, Properties properties, CacheDataDescription metadata) {
		if ( log.isDebugEnabled() ) {
			log.debug( "Building collection cache region [" + regionName + "]" );
		}
		final AdvancedCache cache = getCache( regionName, DataType.COLLECTION, metadata);
		final CollectionRegion region = new CollectionRegionImpl(cache, regionName, transactionManager, metadata, this, getCacheKeysFactory());
      startRegion((BaseRegion) region);
		return region;
	}

	@Override
	public EntityRegion buildEntityRegion(String regionName, Properties properties, CacheDataDescription metadata) {
		if ( log.isDebugEnabled() ) {
			log.debugf(
					"Building entity cache region [%s] (mutable=%s, versioned=%s)",
					regionName,
					metadata.isMutable(),
					metadata.isVersioned()
			);
		}
		final AdvancedCache cache = getCache( regionName, metadata.isMutable() ? DataType.ENTITY : DataType.IMMUTABLE_ENTITY, metadata );
		final EntityRegion region = new EntityRegionImpl(cache, regionName, transactionManager, metadata, this, getCacheKeysFactory());
      startRegion((BaseRegion) region);
		return region;
	}

	@Override
	public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties properties, CacheDataDescription metadata) {
		if ( log.isDebugEnabled() ) {
			log.debug("Building natural id cache region [" + regionName + "]");
		}
		final AdvancedCache cache = getCache( regionName, DataType.NATURAL_ID, metadata);
		NaturalIdRegion region = new NaturalIdRegionImpl(cache, regionName, transactionManager, metadata, this, getCacheKeysFactory());
      startRegion((BaseRegion) region);
		return region;
	}

	@Override
	public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties properties) {
		if ( log.isDebugEnabled() ) {
			log.debug( "Building query results cache region [" + regionName + "]" );
		}

		final AdvancedCache cache = getCache( regionName, DataType.QUERY, null);
		final QueryResultsRegion region = new QueryResultsRegionImpl(cache, regionName, transactionManager, this);
      startRegion((BaseRegion) region);
		return region;
	}

	@Override
	public TimestampsRegion buildTimestampsRegion(String regionName, Properties properties) {
		if ( log.isDebugEnabled() ) {
			log.debug( "Building timestamps cache region [" + regionName + "]" );
		}
		final AdvancedCache cache = getCache( regionName, DataType.TIMESTAMPS, null);
		TimestampsRegion region;
		if ( Caches.isClustered(cache) ) {
			region = new ClusteredTimestampsRegionImpl(cache, regionName, this);
		}
		else {
			region = new TimestampsRegionImpl(cache, regionName, this);
		}
      startRegion((BaseRegion) region);
		return region;
	}

	public Configuration getPendingPutsCacheConfiguration() {
		return dataTypeConfigurations.get(DataType.PENDING_PUTS);
	}

	protected CacheKeysFactory getCacheKeysFactory() {
		return cacheKeysFactory;
	}

	@Override
	public boolean isMinimalPutsEnabledByDefault() {
		// TODO: change to false
		return true;
	}

	@Override
	public AccessType getDefaultAccessType() {
		return AccessType.TRANSACTIONAL;
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
	public void start(SessionFactoryOptions settings, Properties properties) throws CacheException {
		log.debug( "Starting Infinispan region factory" );

		// determine the CacheKeysFactory to use...
		this.cacheKeysFactory = determineCacheKeysFactory( settings, properties );

		try {
			this.settings = settings;
			transactionManagerlookup = createTransactionManagerLookup( settings, properties );
			transactionManager = transactionManagerlookup.getTransactionManager();

			final Enumeration keys = properties.propertyNames();
			while ( keys.hasMoreElements() ) {
				final String key = (String) keys.nextElement();
				int prefixLoc;
				if ( (prefixLoc = key.indexOf( PREFIX )) != -1 ) {
					parseProperty( prefixLoc, key, extractProperty(key, properties));
				}
			}

			String globalStatsProperty = ConfigurationHelper.extractPropertyValue(INFINISPAN_GLOBAL_STATISTICS_PROP, properties);
			globalStats = (globalStatsProperty != null) ? Boolean.valueOf(globalStatsProperty) : null;
			if (properties.containsKey(INFINISPAN_USE_SYNCHRONIZATION_PROP)) {
				log.propertyUseSynchronizationDeprecated();
			}

			ServiceRegistry serviceRegistry = settings.getServiceRegistry();
			manager = createCacheManager(properties, serviceRegistry);
			defineDataTypeCacheConfigurations(serviceRegistry);
		}
		catch (CacheException ce) {
			throw ce;
		}
		catch (Throwable t) {
			throw log.unableToStart(t);
		}
	}

	private CacheKeysFactory determineCacheKeysFactory(SessionFactoryOptions settings, Properties properties) {
		return settings.getServiceRegistry().getService( StrategySelector.class ).resolveDefaultableStrategy(
				CacheKeysFactory.class,
				properties.get( AvailableSettings.CACHE_KEYS_FACTORY ),
				DefaultCacheKeysFactory.INSTANCE
		);
	}

	protected EmbeddedCacheManager createCacheManager(Properties properties, ServiceRegistry serviceRegistry) {
		for (EmbeddedCacheManagerProvider provider : ServiceLoader.load(EmbeddedCacheManagerProvider.class, EmbeddedCacheManagerProvider.class.getClassLoader())) {
			EmbeddedCacheManager cacheManager = provider.getEmbeddedCacheManager(properties);
			if (cacheManager != null) {
				return cacheManager;
			}
		}
		return new DefaultCacheManagerProvider(serviceRegistry).getEmbeddedCacheManager(properties);
	}

	protected org.infinispan.transaction.lookup.TransactionManagerLookup createTransactionManagerLookup(
			SessionFactoryOptions settings, Properties properties) {
		return new HibernateTransactionManagerLookup( settings, properties );
	}

	@Override
	public void stop() {
		log.debug( "Stop region factory" );
		stopCacheRegions();
		stopCacheManager();
	}

	protected void stopCacheRegions() {
		log.debug( "Clear region references" );
		getCacheCommandFactory().clearRegions( regions );
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

	private void startRegion(BaseRegion region) {
		regions.add( region );
		getCacheCommandFactory().addRegion( region );
	}

	private void parseProperty(int prefixLoc, String key, String value) {
		final ConfigurationBuilder builder;
		int suffixLoc;
		if ( (suffixLoc = key.indexOf( CONFIG_SUFFIX )) != -1 && !key.equals( INFINISPAN_CONFIG_RESOURCE_PROP )) {
			String regionName = key.substring( prefixLoc + PREFIX.length(), suffixLoc );
			baseConfigurations.put(regionName, value);
		}
		else if (key.contains(DEPRECATED_STRATEGY_SUFFIX)) {
			log.ignoringDeprecatedProperty(DEPRECATED_STRATEGY_SUFFIX);
		}
		else if ( (suffixLoc = key.indexOf( WAKE_UP_INTERVAL_SUFFIX )) != -1
				|| (suffixLoc = key.indexOf(DEPRECATED_WAKE_UP_INTERVAL_SUFFIX)) != -1 ) {
			builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
			builder.expiration().wakeUpInterval( Long.parseLong(value) );
		}
		else if ( (suffixLoc = key.indexOf(SIZE_SUFFIX)) != -1 ) {
			builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
			builder.memory().size( Long.parseLong(value) );
		}
		else if ( (suffixLoc = key.indexOf(DEPRECATED_MAX_ENTRIES_SUFFIX)) != -1 ) {
			log.deprecatedProperty(DEPRECATED_MAX_ENTRIES_SUFFIX, SIZE_SUFFIX);
			builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
			builder.memory().size( Long.parseLong(value) );
		}
		else if ( (suffixLoc = key.indexOf( LIFESPAN_SUFFIX )) != -1 ) {
			builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
			builder.expiration().lifespan( Long.parseLong(value) );
		}
		else if ( (suffixLoc = key.indexOf( MAX_IDLE_SUFFIX )) != -1 ) {
			builder = getOrCreateConfig( prefixLoc, key, suffixLoc );
			builder.expiration().maxIdle( Long.parseLong(value) );
		}
	}

	private String extractProperty(String key, Properties properties) {
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
			}
			else {
				builder = new ConfigurationBuilder().read(configuration);
			}
			ConfigurationBuilder override = configOverrides.get( type.key );
			if (override != null) {
				builder.read(override.build(false));
			}
			builder.template(true);
			configureTransactionManager( builder );
			dataTypeConfigurations.put(type, builder.build());
		}
	}

	protected AdvancedCache getCache(String regionName, DataType type, CacheDataDescription metadata) {
		if (!manager.cacheExists(regionName)) {
			String templateCacheName = baseConfigurations.get(regionName);
			Configuration configuration = null;
			ConfigurationBuilder builder = new ConfigurationBuilder();
			if (templateCacheName != null) {
				configuration = manager.getCacheConfiguration(templateCacheName);
				if (configuration == null) {
					log.customConfigForRegionNotFound(templateCacheName, regionName, type.key);
				}
				else {
					log.debugf("Region '%s' will use cache template '%s'", regionName, templateCacheName);
					builder.read(configuration);
					configureTransactionManager(builder);
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
			ConfigurationBuilder override = configOverrides.get(regionName);
			if (override != null) {
				log.debugf("Region '%s' has additional configuration set through properties.", regionName);
				builder.read(override.build(false));
			}
			if (globalStats != null) {
				builder.jmxStatistics().enabled(globalStats).available(globalStats);
			}
			configuration = builder.build();
			type.validate(configuration);
			manager.defineConfiguration(regionName, configuration);
		}
		final AdvancedCache cache = manager.getCache( regionName ).getAdvancedCache();
		// TODO: not sure if this is needed in recent Infinispan
		if ( !cache.getStatus().allowInvocations() ) {
			cache.start();
		}
		return cache;
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

	private void configureTransactionManager(ConfigurationBuilder builder) {
		TransactionConfiguration transaction = builder.transaction().create();
		if (transaction.transactionMode().isTransactional() ) {
			final String ispnTmLookupClassName = transaction.transactionManagerLookup().getClass().getName();
			final String hbTmLookupClassName = HibernateTransactionManagerLookup.class.getName();
			if ( GenericTransactionManagerLookup.class.getName().equals( ispnTmLookupClassName ) ) {
				log.debug(
						"Using default Infinispan transaction manager lookup " +
								"instance (GenericTransactionManagerLookup), overriding it " +
								"with Hibernate transaction manager lookup"
				);
				builder.transaction().transactionManagerLookup( transactionManagerlookup );
			}
			else if ( ispnTmLookupClassName != null && !ispnTmLookupClassName.equals( hbTmLookupClassName ) ) {
				log.debug(
						"Infinispan is configured [" + ispnTmLookupClassName + "] with a different transaction manager lookup " +
								"class than Hibernate [" + hbTmLookupClassName + "]"
				);
			}
			else {
				// Infinispan TM lookup class null, so apply Hibernate one directly
				builder.transaction().transactionManagerLookup( transactionManagerlookup );
			}
			builder.transaction().useSynchronization( DEF_USE_SYNCHRONIZATION );
		}
	}

	public SessionFactoryOptions getSettings() {
		return settings;
	}
}
