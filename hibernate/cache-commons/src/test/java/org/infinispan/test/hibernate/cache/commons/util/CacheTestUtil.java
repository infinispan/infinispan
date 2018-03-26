/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.util;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.resource.transaction.backend.jdbc.internal.JdbcResourceLocalTransactionCoordinatorBuilderImpl;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorBuilderImpl;
import org.hibernate.service.ServiceRegistry;

/**
 * Utilities for cache testing.
 *
 * @author <a href="brian.stansberry@jboss.com">Brian Stansberry</a>
 */
public class CacheTestUtil {
	@SuppressWarnings("unchecked")
	public static Map buildBaselineSettings(
			String regionPrefix,
			boolean use2ndLevel,
			boolean useQueries,
			Class<? extends JtaPlatform> jtaPlatform) {
		Map settings = new HashMap();

		settings.put( AvailableSettings.GENERATE_STATISTICS, "true" );
		settings.put( AvailableSettings.USE_STRUCTURED_CACHE, "true" );
		if (jtaPlatform == null || jtaPlatform == NoJtaPlatform.class) {
			settings.put(Environment.TRANSACTION_COORDINATOR_STRATEGY, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class.getName());
			settings.put(AvailableSettings.JTA_PLATFORM, NoJtaPlatform.class);
		} else {
			settings.put(Environment.TRANSACTION_COORDINATOR_STRATEGY, JtaTransactionCoordinatorBuilderImpl.class.getName());
			settings.put(AvailableSettings.JTA_PLATFORM, jtaPlatform);
		}
		settings.put( AvailableSettings.CACHE_REGION_FACTORY, TestRegionFactoryProvider.load().getRegionFactoryClass() );
		settings.put( AvailableSettings.CACHE_REGION_PREFIX, regionPrefix );
		settings.put( AvailableSettings.USE_SECOND_LEVEL_CACHE, String.valueOf( use2ndLevel ) );
		settings.put( AvailableSettings.USE_QUERY_CACHE, String.valueOf( useQueries ) );

		return settings;
	}

	public static StandardServiceRegistryBuilder buildBaselineStandardServiceRegistryBuilder(
			String regionPrefix,
			boolean use2ndLevel,
			boolean useQueries,
			Class<? extends JtaPlatform> jtaPlatform) {
		StandardServiceRegistryBuilder ssrb = new StandardServiceRegistryBuilder();

		ssrb.applySettings(
				  buildBaselineSettings( regionPrefix, use2ndLevel, useQueries, jtaPlatform )
		);

		return ssrb;
	}

	public static StandardServiceRegistryBuilder buildCustomQueryCacheStandardServiceRegistryBuilder(
			  String regionPrefix,
			  String queryCacheName,
			  Class<? extends JtaPlatform> jtaPlatform) {
		final StandardServiceRegistryBuilder ssrb = buildBaselineStandardServiceRegistryBuilder(
				  regionPrefix, true, true, jtaPlatform
		);
		ssrb.applySetting( InfinispanProperties.QUERY_CACHE_RESOURCE_PROP, queryCacheName );
		return ssrb;
	}

	public static <RF extends RegionFactory> RF createRegionFactory(Class<RF> clazz, Properties properties) {
		try {
			try {
				Constructor<RF> constructor = clazz.getConstructor(Properties.class);
				return constructor.newInstance(properties);
			}
			catch (NoSuchMethodException e) {
				return clazz.newInstance();
			}
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static TestRegionFactory startRegionFactory(ServiceRegistry serviceRegistry) {
		try {
			final ConfigurationService cfgService = serviceRegistry.getService( ConfigurationService.class );
			final Properties properties = toProperties( cfgService.getSettings() );

			TestRegionFactory regionFactory = TestRegionFactoryProvider.load().create(properties);
			regionFactory.start( serviceRegistry, properties );

			return regionFactory;
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static TestRegionFactory startRegionFactory(
			  ServiceRegistry serviceRegistry,
			  CacheTestSupport testSupport) {
		TestRegionFactory factory = startRegionFactory( serviceRegistry );
		testSupport.registerFactory( factory );
		return factory;
	}

	public static void stopRegionFactory(
			  TestRegionFactory factory,
			  CacheTestSupport testSupport) {
		testSupport.unregisterFactory( factory );
		factory.stop();
	}

	public static Properties toProperties(Map map) {
		if ( map == null ) {
			return null;
		}

		if ( map instanceof Properties ) {
			return (Properties) map;
		}

		Properties properties = new Properties();
		properties.putAll( map );
		return properties;
	}

	/**
	 * Prevent instantiation.
	 */
	private CacheTestUtil() {
	}
}
