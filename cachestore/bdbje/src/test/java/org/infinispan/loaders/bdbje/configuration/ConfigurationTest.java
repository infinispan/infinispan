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
package org.infinispan.loaders.bdbje.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.bdbje.BdbjeCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.bdbje.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testBdbjeCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(BdbjeCacheStoreConfigurationBuilder.class).location("/tmp/bdbje").cacheDbNamePrefix("myprefix").catalogDbName("mycatalog").fetchPersistentState(true).async().enable();
      Configuration configuration = b.build();
      BdbjeCacheStoreConfiguration store = (BdbjeCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.location().equals("/tmp/bdbje");
      assert store.cacheDbNamePrefix().equals("myprefix");
      assert store.catalogDbName().equals("mycatalog");
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(BdbjeCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      BdbjeCacheStoreConfiguration store2 = (BdbjeCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.location().equals("/tmp/bdbje");
      assert store2.cacheDbNamePrefix().equals("myprefix");
      assert store2.catalogDbName().equals("mycatalog");
      assert store2.fetchPersistentState();
      assert store2.async().enabled();

      BdbjeCacheStoreConfig legacy = store.adapt();
      assert legacy.getLocation().equals("/tmp/bdbje");
      assert legacy.getCacheDbNamePrefix().equals("myprefix");
      assert legacy.getCatalogDbName().equals("mycatalog");
      assert legacy.isFetchPersistentState();
      assert legacy.getAsyncStoreConfig().isEnabled();
   }
}
