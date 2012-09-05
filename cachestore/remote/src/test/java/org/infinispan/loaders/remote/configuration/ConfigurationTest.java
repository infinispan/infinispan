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
package org.infinispan.loaders.remote.configuration;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.remote.RemoteCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.remote.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testRemoteCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(RemoteCacheStoreConfigurationBuilder.class)
         .remoteCacheName("RemoteCache")
         .fetchPersistentState(true)
         .addServer()
            .host("one").port(12111)
         .addServer()
            .host("two")
         .connectionPool()
            .maxActive(10)
            .minIdle(5)
            .exhaustedAction(ExhaustedAction.EXCEPTION)
            .minEvictableIdleTime(10000)
         .async().enable();
      Configuration configuration = b.build();
      RemoteCacheStoreConfiguration store = (RemoteCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.remoteCacheName().equals("RemoteCache");
      assert store.servers().size() == 2;
      assert store.connectionPool().maxActive() == 10;
      assert store.connectionPool().minIdle() == 5;
      assert store.connectionPool().exhaustedAction() == ExhaustedAction.EXCEPTION;
      assert store.connectionPool().minEvictableIdleTime() == 10000;
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(RemoteCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      RemoteCacheStoreConfiguration store2 = (RemoteCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.remoteCacheName().equals("RemoteCache");
      assert store2.servers().size() == 2;
      assert store2.connectionPool().maxActive() == 10;
      assert store2.connectionPool().minIdle() == 5;
      assert store2.connectionPool().exhaustedAction() == ExhaustedAction.EXCEPTION;
      assert store2.connectionPool().minEvictableIdleTime() == 10000;
      assert store2.fetchPersistentState();
      assert store2.async().enabled();

      RemoteCacheStoreConfig legacy = store.adapt();
      assert "RemoteCache".equals(legacy.getRemoteCacheName());
      assert "one:12111;two:11222".equals(legacy.getHotRodClientProperties().get(
            ConfigurationProperties.SERVER_LIST));
      assert legacy.getTypedProperties().getIntProperty("whenExhaustedAction", -1) == 0;
      assert legacy.isFetchPersistentState();
      assert legacy.asyncStore().isEnabled();
   }
}