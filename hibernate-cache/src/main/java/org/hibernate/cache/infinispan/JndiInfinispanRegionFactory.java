/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.cache.infinispan;

import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.infinispan.util.InfinispanMessageLogger;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.internal.util.jndi.JndiHelper;
import org.hibernate.service.ServiceRegistry;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A {@link org.hibernate.cache.spi.RegionFactory} for <a href="http://www.jboss.org/infinispan">Infinispan</a>-backed cache
 * regions that finds its cache manager in JNDI rather than creating one itself.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class JndiInfinispanRegionFactory extends InfinispanRegionFactory {

	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( JndiInfinispanRegionFactory.class );

	/**
	 * Specifies the JNDI name under which the {@link EmbeddedCacheManager} to use is bound.
	 * There is no default value -- the user must specify the property.
	 */
	public static final String CACHE_MANAGER_RESOURCE_PROP = "hibernate.cache.infinispan.cachemanager";

	/**
	 * Constructs a JndiInfinispanRegionFactory
	 */
	@SuppressWarnings("UnusedDeclaration")
	public JndiInfinispanRegionFactory() {
		super();
	}

	/**
	 * Constructs a JndiInfinispanRegionFactory
	 *
	 * @param props Any properties to apply (not used).
	 */
	@SuppressWarnings("UnusedDeclaration")
	public JndiInfinispanRegionFactory(Properties props) {
		super( props );
	}

	@Override
	protected EmbeddedCacheManager createCacheManager(
			Properties properties,
			ServiceRegistry serviceRegistry) throws CacheException {
		final String name = ConfigurationHelper.getString( CACHE_MANAGER_RESOURCE_PROP, properties, null );
		if ( name == null ) {
			throw log.propertyCacheManagerResourceNotSet();
		}
		return locateCacheManager( name, JndiHelper.extractJndiProperties( properties ) );
	}

	private EmbeddedCacheManager locateCacheManager(String jndiNamespace, Properties jndiProperties) {
		Context ctx = null;
		try {
			ctx = new InitialContext( jndiProperties );
			return (EmbeddedCacheManager) ctx.lookup( jndiNamespace );
		}
		catch (NamingException ne) {
			throw log.unableToRetrieveCmFromJndi(jndiNamespace);
		}
		finally {
			if ( ctx != null ) {
				try {
					ctx.close();
				}
				catch (NamingException ne) {
					log.unableToReleaseContext(ne);
				}
			}
		}
	}

	@Override
	public void stop() {
		// Do not attempt to stop a cache manager because it wasn't created by this region factory.
	}
}
