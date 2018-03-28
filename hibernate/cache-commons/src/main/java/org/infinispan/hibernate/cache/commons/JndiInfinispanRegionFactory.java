/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons;

import java.util.Properties;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A {@link org.hibernate.cache.spi.RegionFactory} for <a href="http://www.jboss.org/infinispan">Infinispan</a>-backed cache
 * regions that finds its cache manager in JNDI rather than creating one itself.
 *
 * @author Galder Zamarreño
 * @since 3.5
 * @deprecated Replaced by {@link JndiCacheManagerProvider}
 */
@Deprecated
public class JndiInfinispanRegionFactory extends InfinispanRegionFactory {

	/**
	 * Specifies the JNDI name under which the {@link EmbeddedCacheManager} to use is bound.
	 * There is no default value -- the user must specify the property.
	 */
	public static final String CACHE_MANAGER_RESOURCE_PROP = JndiCacheManagerProvider.CACHE_MANAGER_RESOURCE_PROP;

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
}
