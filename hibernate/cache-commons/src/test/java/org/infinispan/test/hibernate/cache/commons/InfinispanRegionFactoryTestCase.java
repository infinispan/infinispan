package org.infinispan.test.hibernate.cache.commons;

import static org.infinispan.hibernate.cache.spi.InfinispanProperties.DEF_INFINISPAN_CONFIG_RESOURCE;
import static org.infinispan.hibernate.cache.spi.InfinispanProperties.DEF_PENDING_PUTS_RESOURCE;
import static org.infinispan.hibernate.cache.spi.InfinispanProperties.DEF_TIMESTAMPS_RESOURCE;
import static org.infinispan.hibernate.cache.spi.InfinispanProperties.INFINISPAN_CONFIG_RESOURCE_PROP;
import static org.infinispan.hibernate.cache.spi.InfinispanProperties.TIMESTAMPS_CACHE_RESOURCE_PROP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.function.Consumer;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Environment;
import org.hibernate.testing.boot.ServiceRegistryTestingImpl;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusteringConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.hibernate.cache.commons.util.InfinispanTestingSetup;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.transaction.TransactionMode;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

/**
 * InfinispanRegionFactoryTestCase.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class InfinispanRegionFactoryTestCase  {
   @Rule
   public InfinispanTestingSetup infinispanTestIdentifier = new InfinispanTestingSetup();

   private final ServiceRegistryTestingImpl serviceRegistry = ServiceRegistryTestingImpl.forUnitTesting();

   @After
   public void tearDown() {
      serviceRegistry.destroy();
   }

   @Test
	public void testConfigurationProcessing() {
		final String person = "com.acme.Person";
		final String addresses = "com.acme.Person.addresses";
		Properties p = createProperties();
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.cfg", "person-cache");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.memory.size", "5000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.wake_up_interval", "2000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.lifespan", "60000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.max_idle", "30000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.cfg", "person-addresses-cache");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.expiration.lifespan", "120000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.expiration.max_idle", "60000");
		p.setProperty("hibernate.cache.infinispan.query.cfg", "my-query-cache");
		p.setProperty("hibernate.cache.infinispan.query.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.query.expiration.wake_up_interval", "3000");
		p.setProperty("hibernate.cache.infinispan.query.memory.size", "10000");

		TestRegionFactory factory = createRegionFactory(p);

		try {
			assertEquals("person-cache", factory.getBaseConfiguration(person));
			Configuration personOverride = factory.getConfigurationOverride(person);
			assertEquals(5000, personOverride.memory().maxCount());
			assertEquals(2000, personOverride.expiration().wakeUpInterval());
			assertEquals(60000, personOverride.expiration().lifespan());
			assertEquals(30000, personOverride.expiration().maxIdle());

			assertEquals("person-addresses-cache", factory.getBaseConfiguration(addresses));
			Configuration addressesOverride = factory.getConfigurationOverride(addresses);
			assertEquals(120000, addressesOverride.expiration().lifespan());
			assertEquals(60000, addressesOverride.expiration().maxIdle());

			assertEquals("my-query-cache", factory.getBaseConfiguration(InfinispanProperties.QUERY));
			Configuration queryOverride = factory.getConfigurationOverride(InfinispanProperties.QUERY);
			assertEquals(10000, queryOverride.memory().maxCount());
			assertEquals(3000, queryOverride.expiration().wakeUpInterval());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testBuildEntityCollectionRegionsPersonPlusEntityCollectionOverrides() {
		final String person = "com.acme.Person";
		final String address = "com.acme.Address";
		final String car = "com.acme.Car";
		final String addresses = "com.acme.Person.addresses";
		final String parts = "com.acme.Car.parts";
		Properties p = createProperties();
		// First option, cache defined for entity and overrides for generic entity data type and entity itself.
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.cfg", "person-cache");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.memory.size", "5000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.wake_up_interval", "2000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.lifespan", "60000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.max_idle", "30000");
		p.setProperty("hibernate.cache.infinispan.entity.cfg", "myentity-cache");
		p.setProperty("hibernate.cache.infinispan.entity.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.entity.expiration.wake_up_interval", "3000");
		p.setProperty("hibernate.cache.infinispan.entity.memory.size", "20000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.cfg", "addresses-cache");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.memory.size", "5500");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.expiration.wake_up_interval", "2500");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.expiration.lifespan", "65000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.addresses.expiration.max_idle", "35000");
		p.setProperty("hibernate.cache.infinispan.collection.cfg", "mycollection-cache");
		p.setProperty("hibernate.cache.infinispan.collection.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.collection.expiration.wake_up_interval", "3500");
		p.setProperty("hibernate.cache.infinispan.collection.memory.size", "25000");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			EmbeddedCacheManager manager = factory.getCacheManager();
			assertFalse(manager.getCacheManagerConfiguration().jmx().enabled());
			assertNotNull(factory.getBaseConfiguration(person));
			assertFalse(isDefinedCache(factory, person));
			assertNotNull(factory.getBaseConfiguration(addresses));
			assertFalse(isDefinedCache(factory, addresses));
			assertNull(factory.getBaseConfiguration(address));
			assertNull(factory.getBaseConfiguration(parts));
			AdvancedCache cache;

         InfinispanBaseRegion region = factory.buildEntityRegion(person, AccessType.TRANSACTIONAL);
			assertTrue(isDefinedCache(factory, person));
			cache = region.getCache();
			Configuration cacheCfg = cache.getCacheConfiguration();
			assertEquals(2000, cacheCfg.expiration().wakeUpInterval());
			assertEquals(5000, cacheCfg.memory().maxCount());
			assertEquals(60000, cacheCfg.expiration().lifespan());
			assertEquals(30000, cacheCfg.expiration().maxIdle());
			assertFalse(cacheCfg.statistics().enabled());

         region = factory.buildEntityRegion(address, AccessType.TRANSACTIONAL);
			assertTrue(isDefinedCache(factory, person));
			cache = region.getCache();
			cacheCfg = cache.getCacheConfiguration();
			assertEquals(3000, cacheCfg.expiration().wakeUpInterval());
			assertEquals(20000, cacheCfg.memory().maxCount());
			assertFalse(cacheCfg.statistics().enabled());

         region = factory.buildEntityRegion(car, AccessType.TRANSACTIONAL);
			assertTrue(isDefinedCache(factory, person));
			cache = region.getCache();
			cacheCfg = cache.getCacheConfiguration();
			assertEquals(3000, cacheCfg.expiration().wakeUpInterval());
			assertEquals(20000, cacheCfg.memory().maxCount());
			assertFalse(cacheCfg.statistics().enabled());

			InfinispanBaseRegion collectionRegion = factory.buildCollectionRegion(addresses, AccessType.TRANSACTIONAL);
			assertTrue(isDefinedCache(factory, person));

			cache = collectionRegion .getCache();
			cacheCfg = cache.getCacheConfiguration();
			assertEquals(2500, cacheCfg.expiration().wakeUpInterval());
			assertEquals(5500, cacheCfg.memory().maxCount());
			assertEquals(65000, cacheCfg.expiration().lifespan());
			assertEquals(35000, cacheCfg.expiration().maxIdle());
			assertFalse(cacheCfg.statistics().enabled());

			collectionRegion = factory.buildCollectionRegion(parts, AccessType.TRANSACTIONAL);
			assertTrue(isDefinedCache(factory, addresses));
			cache = collectionRegion.getCache();
			cacheCfg = cache.getCacheConfiguration();
			assertEquals(3500, cacheCfg.expiration().wakeUpInterval());
			assertEquals(25000, cacheCfg.memory().maxCount());
			assertFalse(cacheCfg.statistics().enabled());

			collectionRegion = factory.buildCollectionRegion(parts, AccessType.TRANSACTIONAL);
			assertTrue(isDefinedCache(factory, addresses));
			cache = collectionRegion.getCache();
			cacheCfg = cache.getCacheConfiguration();
			assertEquals(3500, cacheCfg.expiration().wakeUpInterval());
			assertEquals(25000, cacheCfg.memory().maxCount());
			assertFalse(cacheCfg.statistics().enabled());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testBuildEntityCollectionRegionOverridesOnly() {
		final String address = "com.acme.Address";
		final String personAddressses = "com.acme.Person.addresses";
		AdvancedCache cache;
		Properties p = createProperties();
		p.setProperty("hibernate.cache.infinispan.entity.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.entity.memory.size", "30000");
		p.setProperty("hibernate.cache.infinispan.entity.expiration.wake_up_interval", "3000");
		p.setProperty("hibernate.cache.infinispan.collection.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.collection.memory.size", "35000");
		p.setProperty("hibernate.cache.infinispan.collection.expiration.wake_up_interval", "3500");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			factory.getCacheManager();
			InfinispanBaseRegion region = factory.buildEntityRegion(address, AccessType.TRANSACTIONAL);
			assertNull(factory.getBaseConfiguration(address));
			cache = region.getCache();
			Configuration cacheCfg = cache.getCacheConfiguration();
			assertEquals(3000, cacheCfg.expiration().wakeUpInterval());
			assertEquals(30000, cacheCfg.memory().maxCount());
			// Max idle value comes from base XML configuration
			assertEquals(100000, cacheCfg.expiration().maxIdle());

			InfinispanBaseRegion collectionRegion = factory.buildCollectionRegion(personAddressses, AccessType.TRANSACTIONAL);
			assertNull(factory.getBaseConfiguration(personAddressses));
			cache = collectionRegion.getCache();
			cacheCfg = cache.getCacheConfiguration();
			assertEquals(3500, cacheCfg.expiration().wakeUpInterval());
			assertEquals(35000, cacheCfg.memory().maxCount());
			assertEquals(100000, cacheCfg.expiration().maxIdle());
		} finally {
			factory.stop();
		}
	}
	@Test
	public void testBuildEntityRegionPersonPlusEntityOverridesWithoutCfg() {
		final String person = "com.acme.Person";
		Properties p = createProperties();
		// Third option, no cache defined for entity and overrides for generic entity data type and entity itself.
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.lifespan", "60000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.max_idle", "30000");
		p.setProperty("hibernate.cache.infinispan.entity.cfg", "myentity-cache");
		p.setProperty("hibernate.cache.infinispan.entity.memory.eviction.type", "FIFO");
		p.setProperty("hibernate.cache.infinispan.entity.memory.size", "10000");
		p.setProperty("hibernate.cache.infinispan.entity.expiration.wake_up_interval", "3000");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			factory.getCacheManager();
			assertFalse( isDefinedCache(factory, person ) );
			InfinispanBaseRegion region = factory.buildEntityRegion(person, AccessType.TRANSACTIONAL);
			assertTrue( isDefinedCache(factory, person ) );
			AdvancedCache cache = region.getCache();
			Configuration cacheCfg = cache.getCacheConfiguration();
			assertEquals(3000, cacheCfg.expiration().wakeUpInterval());
			assertEquals(10000, cacheCfg.memory().maxCount());
			assertEquals(60000, cacheCfg.expiration().lifespan());
			assertEquals(30000, cacheCfg.expiration().maxIdle());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testBuildImmutableEntityRegion() {
		AdvancedCache cache;
		Properties p = new Properties();
		TestRegionFactory factory = createRegionFactory(p);
		try {
			factory.getCacheManager();
			InfinispanBaseRegion region = factory.buildEntityRegion("com.acme.Address", AccessType.TRANSACTIONAL);
			assertNull( factory.getBaseConfiguration( "com.acme.Address" ) );
			cache = region.getCache();
			Configuration cacheCfg = cache.getCacheConfiguration();
			assertEquals("Immutable entity should get non-transactional cache", TransactionMode.NON_TRANSACTIONAL, cacheCfg.transaction().transactionMode());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testTimestampValidation() throws IOException {
		final String timestamps = "org.hibernate.cache.spi.UpdateTimestampsCache";
		Properties p = createProperties();
      URL url = FileLookupFactory.newInstance().lookupFileLocation(DEF_INFINISPAN_CONFIG_RESOURCE, getClass().getClassLoader());
      ConfigurationBuilderHolder cbh = new ParserRegistry().parse(url);
      ConfigurationBuilder builder = cbh.getNamedConfigurationBuilders().get( DEF_TIMESTAMPS_RESOURCE );
		builder.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
      DefaultCacheManager manager = new DefaultCacheManager(cbh, true);
		try {
			TestRegionFactory factory = createRegionFactory(manager, p, null);
         factory.start(serviceRegistry, p);
			// Should have failed saying that invalidation is not allowed for timestamp caches.
			Exceptions.expectException(CacheException.class, () -> factory.buildTimestampsRegion(timestamps));
		} finally {
			TestingUtil.killCacheManagers( manager );
		}
	}

	@Test
	public void testBuildDefaultTimestampsRegion() {
		final String timestamps = "org.hibernate.cache.spi.UpdateTimestampsCache";
		Properties p = createProperties();
		TestRegionFactory factory = createRegionFactory(p);
		try {
			assertTrue(isDefinedCache(factory, DEF_TIMESTAMPS_RESOURCE));
			InfinispanBaseRegion region = factory.buildTimestampsRegion(timestamps);
			AdvancedCache cache = region.getCache();
			assertEquals(timestamps, cache.getName());
			Configuration cacheCfg = cache.getCacheConfiguration();
			assertEquals( CacheMode.REPL_ASYNC, cacheCfg.clustering().cacheMode() );
			assertFalse( cacheCfg.statistics().enabled() );
		} finally {
			factory.stop();
		}
	}

	protected boolean isDefinedCache(TestRegionFactory factory, String cacheName) {
		return factory.getCacheManager().getCacheConfiguration(cacheName) != null;
	}

	@Test
	public void testBuildDiffCacheNameTimestampsRegion() {
		final String timestamps = "org.hibernate.cache.spi.UpdateTimestampsCache";
		final String unrecommendedTimestamps = "unrecommended-timestamps";
		Properties p = createProperties();
		p.setProperty( TIMESTAMPS_CACHE_RESOURCE_PROP, unrecommendedTimestamps);
		TestRegionFactory factory = createRegionFactory(p, m -> {
			ConfigurationBuilder builder = new ConfigurationBuilder();
			builder.clustering().stateTransfer().fetchInMemoryState(true);
			builder.clustering().cacheMode( CacheMode.REPL_SYNC );
			m.defineConfiguration(unrecommendedTimestamps, builder.build() );
		});
		try {
			assertEquals(unrecommendedTimestamps, factory.getBaseConfiguration(InfinispanProperties.TIMESTAMPS));
			InfinispanBaseRegion region = factory.buildTimestampsRegion(timestamps);
			AdvancedCache cache = region.getCache();
			Configuration cacheCfg = cache.getCacheConfiguration();
			assertEquals(CacheMode.REPL_SYNC, cacheCfg.clustering().cacheMode());
         assertNotSame(StorageType.OFF_HEAP, cacheCfg.memory().storage());
			assertFalse(cacheCfg.statistics().enabled());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testBuildTimestampsRegionWithCacheNameOverride() {
		final String timestamps = "org.hibernate.cache.spi.UpdateTimestampsCache";
		final String myTimestampsCache = "mytimestamps-cache";
		Properties p = createProperties();
		p.setProperty(TIMESTAMPS_CACHE_RESOURCE_PROP, myTimestampsCache);
		TestRegionFactory factory = createRegionFactory(p, m -> {
			ClusteringConfigurationBuilder builder = new ConfigurationBuilder().clustering().cacheMode(CacheMode.LOCAL);
			m.defineConfiguration(myTimestampsCache, builder.build());
		});
		try {
			InfinispanBaseRegion region = factory.buildTimestampsRegion(timestamps);
			assertTrue(isDefinedCache(factory, timestamps));
			// default timestamps cache is async replicated
			assertEquals(CacheMode.LOCAL, region.getCache().getCacheConfiguration().clustering().cacheMode());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testBuildTimestampsRegionWithFifoEvictionOverride() {
		final String timestamps = "org.hibernate.cache.spi.UpdateTimestampsCache";
		final String myTimestampsCache = "mytimestamps-cache";
		Properties p = createProperties();
		p.setProperty(TIMESTAMPS_CACHE_RESOURCE_PROP, myTimestampsCache);
		p.setProperty("hibernate.cache.infinispan.timestamps.memory.eviction.type", "FIFO");
		p.setProperty("hibernate.cache.infinispan.timestamps.memory.size", "10000");
		p.setProperty("hibernate.cache.infinispan.timestamps.expiration.wake_up_interval", "3000");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			Exceptions.expectException(CacheException.class, () -> factory.buildTimestampsRegion(timestamps));
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testBuildTimestampsRegionWithNoneEvictionOverride() {
		final String timestamps = "org.hibernate.cache.spi.UpdateTimestampsCache";
		final String timestampsNoEviction = "timestamps-no-eviction";
		Properties p = createProperties();
		p.setProperty("hibernate.cache.infinispan.timestamps.cfg", timestampsNoEviction);
		p.setProperty("hibernate.cache.infinispan.timestamps.memory.size", "0");
		p.setProperty("hibernate.cache.infinispan.timestamps.expiration.wake_up_interval", "3000");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			InfinispanBaseRegion region = factory.buildTimestampsRegion(timestamps);
			assertTrue( isDefinedCache(factory, timestamps) );
			assertEquals(3000, region.getCache().getCacheConfiguration().expiration().wakeUpInterval());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testBuildQueryRegion() {
		final String query = "org.hibernate.cache.internal.StandardQueryCache";
		Properties p = createProperties();
		TestRegionFactory factory = createRegionFactory(p);
		try {
			assertTrue(isDefinedCache(factory, "local-query"));
			InfinispanBaseRegion region = factory.buildQueryResultsRegion(query);
			AdvancedCache cache = region.getCache();
			Configuration cacheCfg = cache.getCacheConfiguration();
			assertEquals( CacheMode.LOCAL, cacheCfg.clustering().cacheMode() );
			assertFalse( cacheCfg.statistics().enabled() );
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testBuildQueryRegionWithCustomRegionName() {
		final String queryRegionName = "myquery";
		Properties p = createProperties();
		p.setProperty("hibernate.cache.infinispan.myquery.cfg", "timestamps-none-eviction");
		p.setProperty("hibernate.cache.infinispan.myquery.memory.eviction.type", "MEMORY");
		p.setProperty("hibernate.cache.infinispan.myquery.expiration.wake_up_interval", "2222");
		p.setProperty("hibernate.cache.infinispan.myquery.memory.size", "11111");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			assertTrue(isDefinedCache(factory, "local-query"));
			InfinispanBaseRegion region = factory.buildQueryResultsRegion(queryRegionName);
			assertNotNull(factory.getBaseConfiguration(queryRegionName));
			assertTrue(isDefinedCache(factory, queryRegionName));
			AdvancedCache cache = region.getCache();
			Configuration cacheCfg = cache.getCacheConfiguration();
			assertEquals(2222, cacheCfg.expiration().wakeUpInterval());
			assertEquals( 11111, cacheCfg.memory().maxCount() );
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testEnableStatistics() {
		Properties p = createProperties();
		p.setProperty("hibernate.cache.infinispan.statistics", "true");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.lifespan", "60000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.max_idle", "30000");
		p.setProperty("hibernate.cache.infinispan.entity.cfg", "myentity-cache");
		p.setProperty("hibernate.cache.infinispan.entity.memory.eviction.type", "FIFO");
		p.setProperty("hibernate.cache.infinispan.entity.expiration.wake_up_interval", "3000");
		p.setProperty("hibernate.cache.infinispan.entity.memory.size", "10000");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			EmbeddedCacheManager manager = factory.getCacheManager();
			assertTrue(manager.getCacheManagerConfiguration().jmx().enabled());
			InfinispanBaseRegion region = factory.buildEntityRegion("com.acme.Address", AccessType.TRANSACTIONAL);
			AdvancedCache cache = region.getCache();
			assertTrue(cache.getCacheConfiguration().statistics().enabled());

			region = factory.buildEntityRegion("com.acme.Person", AccessType.TRANSACTIONAL);
			cache = region.getCache();
			assertTrue(cache.getCacheConfiguration().statistics().enabled());

			final String query = "org.hibernate.cache.internal.StandardQueryCache";
			InfinispanBaseRegion queryRegion = factory.buildQueryResultsRegion(query);
			cache = queryRegion.getCache();
			assertTrue(cache.getCacheConfiguration().statistics().enabled());

			final String timestamps = "org.hibernate.cache.spi.UpdateTimestampsCache";
			InfinispanBaseRegion timestampsRegion = factory.buildTimestampsRegion(timestamps);
			cache = timestampsRegion.getCache();
			assertTrue(cache.getCacheConfiguration().statistics().enabled());

			InfinispanBaseRegion collectionRegion = factory.buildCollectionRegion("com.acme.Person.addresses", AccessType.TRANSACTIONAL);
			cache = collectionRegion.getCache();
			assertTrue(cache.getCacheConfiguration().statistics().enabled());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testDisableStatistics() {
		Properties p = createProperties();
		p.setProperty("hibernate.cache.infinispan.statistics", "false");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.lifespan", "60000");
		p.setProperty("hibernate.cache.infinispan.com.acme.Person.expiration.max_idle", "30000");
		p.setProperty("hibernate.cache.infinispan.entity.cfg", "myentity-cache");
		p.setProperty("hibernate.cache.infinispan.entity.memory.eviction.type", "FIFO");
		p.setProperty("hibernate.cache.infinispan.entity.expiration.wake_up_interval", "3000");
		p.setProperty("hibernate.cache.infinispan.entity.memory.size", "10000");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			InfinispanBaseRegion region = factory.buildEntityRegion("com.acme.Address", AccessType.TRANSACTIONAL);
			AdvancedCache cache = region.getCache();
			assertFalse( cache.getCacheConfiguration().statistics().enabled() );

			region = factory.buildEntityRegion("com.acme.Person", AccessType.TRANSACTIONAL);
			cache = region.getCache();
			assertFalse( cache.getCacheConfiguration().statistics().enabled() );

			final String query = "org.hibernate.cache.internal.StandardQueryCache";
			InfinispanBaseRegion queryRegion = factory.buildQueryResultsRegion(query);
			cache = queryRegion.getCache();
			assertFalse( cache.getCacheConfiguration().statistics().enabled() );

			final String timestamps = "org.hibernate.cache.spi.UpdateTimestampsCache";
			InfinispanBaseRegion timestampsRegion = factory.buildTimestampsRegion(timestamps);
			cache = timestampsRegion.getCache();
			assertFalse( cache.getCacheConfiguration().statistics().enabled() );

			InfinispanBaseRegion collectionRegion = factory.buildCollectionRegion("com.acme.Person.addresses", AccessType.TRANSACTIONAL);
			cache = collectionRegion.getCache();
			assertFalse( cache.getCacheConfiguration().statistics().enabled() );
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testDefaultPendingPutsCache() {
		Properties p = createProperties();
		TestRegionFactory factory = createRegionFactory(p);
		try {
			Configuration ppConfig = factory.getCacheManager().getCacheConfiguration(DEF_PENDING_PUTS_RESOURCE);

			assertTrue(ppConfig.isTemplate());
			assertFalse(ppConfig.clustering().cacheMode().isClustered());
			assertTrue(ppConfig.simpleCache());
			assertEquals(TransactionMode.NON_TRANSACTIONAL, ppConfig.transaction().transactionMode());
			assertEquals(60000, ppConfig.expiration().maxIdle());
			assertFalse(ppConfig.statistics().enabled());
		} finally {
			factory.stop();
		}
	}

	@Test
	public void testCustomPendingPutsCache() {
		Properties p = createProperties();
		p.setProperty(INFINISPAN_CONFIG_RESOURCE_PROP, "alternative-infinispan-configs.xml");
		TestRegionFactory factory = createRegionFactory(p);
		try {
			Configuration ppConfig = factory.getCacheManager().getCacheConfiguration(DEF_PENDING_PUTS_RESOURCE);
			assertEquals(120000, ppConfig.expiration().maxIdle());
		} finally {
			factory.stop();
		}
	}

	private TestRegionFactory createRegionFactory(Properties p) {
		return createRegionFactory(null, p, null);
	}

	private TestRegionFactory createRegionFactory(Properties p,
		   Consumer<EmbeddedCacheManager> hook) {
		return createRegionFactory(null, p, hook);
	}

	private TestRegionFactory createRegionFactory(final EmbeddedCacheManager manager, Properties p,
		   Consumer<EmbeddedCacheManager> hook) {
		if (manager != null) {
			p.put(TestRegionFactory.MANAGER, manager);
		}
		if (hook != null) {
			p.put(TestRegionFactory.AFTER_MANAGER_CREATED, hook);
		}
		final TestRegionFactory factory = TestRegionFactoryProvider.load().create(p);
		factory.start(serviceRegistry, p);
		return factory;
	}

	private static Properties createProperties() {
		final Properties properties = new Properties();
		// If configured in the environment, add configuration file name to properties.
		final String cfgFileName =
				  (String) Environment.getProperties().get( INFINISPAN_CONFIG_RESOURCE_PROP );
		if ( cfgFileName != null ) {
			properties.put( INFINISPAN_CONFIG_RESOURCE_PROP, cfgFileName );
		}
		return properties;
	}
}
