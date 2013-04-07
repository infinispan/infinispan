/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.client.hotrod;

import java.util.Set;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * in bulk.
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 */
@Test(testName = "client.hotrod.BulkGetKeysDistTest", groups = "functional")
public class BulkGetKeysDistTest extends BaseBulkGetKeysTest {
	@Override
	protected int numberOfHotRodServers() {
		return 3;
	}

	@Override
	protected ConfigurationBuilder clusterConfig() {
		return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(
				CacheMode.DIST_SYNC, false));
	}
	
	public void testDistribution() {
		for (int i = 0; i < 100; i++) {
			remoteCache.put(i, i);
		}
		
		for (int i = 0 ; i < numberOfHotRodServers(); i++) {
			assert cache(i).size() < 100;
		}
		
		Set<Object> set = remoteCache.keySet();
		assert set.size() == 100;
		for (int i = 0; i < 100; i++) {
			assert set.contains(i);
		}
	}
}
