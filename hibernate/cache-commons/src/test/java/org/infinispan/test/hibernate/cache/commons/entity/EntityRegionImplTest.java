/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.entity;

import java.util.Properties;

import org.hibernate.cache.spi.access.AccessType;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.test.hibernate.cache.commons.AbstractEntityCollectionRegionTest;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;

import static org.junit.Assert.assertNotNull;

/**
 * Tests of EntityRegionImpl.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class EntityRegionImplTest extends AbstractEntityCollectionRegionTest {
	protected static final String CACHE_NAME = "test";

	@Override
	protected void supportedAccessTypeTest(TestRegionFactory regionFactory, Properties properties) {
		InfinispanBaseRegion region = regionFactory.buildEntityRegion("test", accessType);
		assertNotNull(TEST_SESSION_ACCESS.entityAccess(region, accessType));
		regionFactory.getCacheManager().administration().removeCache(CACHE_NAME);
	}

	private TestSessionAccess.TestRegionAccessStrategy entityAccess(InfinispanBaseRegion region) {
		Object access = TEST_SESSION_ACCESS.entityAccess(region, AccessType.TRANSACTIONAL);
		return TEST_SESSION_ACCESS.fromAccess(access);
	}

	@Override
	protected InfinispanBaseRegion createRegion(TestRegionFactory regionFactory, String regionName) {
		return regionFactory.buildEntityRegion(regionName, accessType);
	}

}
