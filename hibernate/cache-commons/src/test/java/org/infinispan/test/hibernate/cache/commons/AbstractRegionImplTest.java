/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons;

import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;

/**
 * Base class for tests of Region implementations.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class AbstractRegionImplTest extends AbstractNonFunctionalTest {

   protected abstract InfinispanBaseRegion createRegion(TestRegionFactory regionFactory, String regionName);

}
