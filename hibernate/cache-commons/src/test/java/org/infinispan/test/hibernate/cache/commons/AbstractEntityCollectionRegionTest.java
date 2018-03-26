/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons;

import java.util.Properties;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cache.spi.access.AccessType;
import org.infinispan.test.hibernate.cache.commons.util.CacheTestUtil;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.junit.Test;

/**
 * Base class for tests of EntityRegion and CollectionRegion implementations.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class AbstractEntityCollectionRegionTest extends AbstractRegionImplTest {
	@Test
	public void testSupportedAccessTypes() {
		StandardServiceRegistryBuilder ssrb = createStandardServiceRegistryBuilder();
		final StandardServiceRegistry registry = ssrb.build();
		try {
			TestRegionFactory regionFactory = CacheTestUtil.startRegionFactory(
					registry,
					getCacheTestSupport()
			);
			supportedAccessTypeTest( regionFactory, CacheTestUtil.toProperties( ssrb.getSettings() ) );
		}
		finally {
			StandardServiceRegistryBuilder.destroy( registry );
		}
	}

	/**
	 * Creates a Region using the given factory, and then ensure that it handles calls to
	 * buildAccessStrategy as expected when all the various {@link AccessType}s are passed as
	 * arguments.
	 */
	protected abstract void supportedAccessTypeTest(TestRegionFactory regionFactory, Properties properties);
}
