/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.collection;

import java.util.Properties;

import org.hibernate.cache.spi.access.AccessType;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.test.hibernate.cache.commons.AbstractEntityCollectionRegionTest;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess.TestRegionAccessStrategy;

import static org.junit.Assert.assertNotNull;

/**
 * @author Galder Zamarreño
 */
public class CollectionRegionImplTest extends AbstractEntityCollectionRegionTest {
	protected static final String CACHE_NAME = "test";

	@Override
	protected void supportedAccessTypeTest(TestRegionFactory regionFactory, Properties properties) {
		InfinispanBaseRegion region = regionFactory.buildCollectionRegion(CACHE_NAME, accessType);
		assertNotNull(TEST_SESSION_ACCESS.collectionAccess(region, accessType));
		regionFactory.getCacheManager().administration().removeCache(CACHE_NAME);
	}

	@Override
	protected InfinispanBaseRegion createRegion(TestRegionFactory regionFactory, String regionName) {
		return regionFactory.buildCollectionRegion(regionName, accessType);
	}

	private TestRegionAccessStrategy collectionAccess(InfinispanBaseRegion region) {
		Object access = TEST_SESSION_ACCESS.collectionAccess(region, AccessType.TRANSACTIONAL);
		return TEST_SESSION_ACCESS.fromAccess(access);
	}

}
