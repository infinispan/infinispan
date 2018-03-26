/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.collection;

import org.infinispan.test.hibernate.cache.commons.AbstractExtraAPITest;

/**
 * @author Galder Zamarreño
 * @since 3.5
 */
public class CollectionRegionAccessExtraAPITest extends AbstractExtraAPITest<Object> {
	@Override
	protected Object getAccessStrategy() {
		return TEST_SESSION_ACCESS.collectionAccess(environment.getCollectionRegion( REGION_NAME, accessType), accessType);
	}
}
