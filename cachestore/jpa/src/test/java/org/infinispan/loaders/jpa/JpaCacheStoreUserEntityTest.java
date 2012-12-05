/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
 * 
 */
package org.infinispan.loaders.jpa;

import org.hibernate.ejb.HibernateEntityManagerFactory;
import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jpa.entity.User;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test (groups = "functional", testName = "loaders.jdbc.binary.JpaCacheStoreEmfTest")
public class JpaCacheStoreUserEntityTest extends BaseJpaCacheStoreTest {

	@Override
	protected CacheStore createCacheStore() throws Exception {
		JpaCacheStoreConfig config = new JpaCacheStoreConfig();
		
		config.setPersistenceUnitName("org.infinispan.loaders.jpa");
		config.setEntityClass(User.class);
		config.setPurgeSynchronously(true);
		
		JpaCacheStore store = new JpaCacheStore();
		store.init(config, cm.getCache(), getMarshaller());
		store.start();
		
		assert store.getEntityManagerFactory() != null;
		assert store.getEntityManagerFactory() instanceof HibernateEntityManagerFactory;
		
		return store;
	}
	
	public void testSimple() throws Exception {
		CacheContainer cm = null;
		try {
			assert cs.getCacheStoreConfig() instanceof JpaCacheStoreConfig;
		} finally {
			TestingUtil.killCacheManagers(cm);
		}
	}

	@Override
	protected TestObject createTestObject(String suffix) {
		User user = new User();
		user.setUsername("u_" + suffix);
		user.setFirstName("fn_" + suffix);
		user.setLastName("ln_" + suffix);
		user.setNote("Some notes " + suffix);
		
		return new TestObject(user.getUsername(), user);
	}
}
