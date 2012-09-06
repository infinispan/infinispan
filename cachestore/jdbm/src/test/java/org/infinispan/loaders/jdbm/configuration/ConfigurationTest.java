/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.jdbm.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.jdbm.JdbmCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.jdbm.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testBdbjeCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(JdbmCacheStoreConfigurationBuilder.class).location("/tmp/jdbm").expiryQueueSize(100).fetchPersistentState(true).async().enable();
      Configuration configuration = b.build();
      JdbmCacheStoreConfiguration store = (JdbmCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.location().equals("/tmp/jdbm");
      assert store.expiryQueueSize() == 100;
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(JdbmCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbmCacheStoreConfiguration store2 = (JdbmCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.location().equals("/tmp/jdbm");
      assert store2.expiryQueueSize() == 100;
      assert store2.fetchPersistentState();
      assert store2.async().enabled();

      JdbmCacheStoreConfig legacy = store.adapt();
      assert legacy.getLocation().equals("/tmp/jdbm");
      assert legacy.getExpiryQueueSize() == 100;
      assert legacy.isFetchPersistentState();
      assert legacy.getAsyncStoreConfig().isEnabled();
   }
}