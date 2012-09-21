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
package org.infinispan.loaders.cloud.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.cloud.CloudCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cloud.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testBdbjeCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(CloudCacheStoreConfigurationBuilder.class)
         .cloudService("transient")
         .identity("me")
         .password("s3cr3t")
         .proxyHost("my-proxy")
         .proxyPort(8080)
         .secure(true)
         .fetchPersistentState(true)
         .async().enable();
      Configuration configuration = b.build();
      CloudCacheStoreConfiguration store = (CloudCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.cloudService().equals("transient");
      assert store.identity().equals("me");
      assert store.password().equals("s3cr3t");
      assert store.proxyHost().equals("my-proxy");
      assert store.proxyPort() == 8080;
      assert store.secure();
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(CloudCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      CloudCacheStoreConfiguration store2 = (CloudCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.cloudService().equals("transient");
      assert store2.identity().equals("me");
      assert store2.password().equals("s3cr3t");
      assert store2.proxyHost().equals("my-proxy");
      assert store2.proxyPort() == 8080;
      assert store2.secure();
      assert store2.fetchPersistentState();
      assert store2.async().enabled();

      CloudCacheStoreConfig legacy = store.adapt();
      assert legacy.getCloudService().equals("transient");
      assert legacy.getIdentity().equals("me");
      assert legacy.getPassword().equals("s3cr3t");
      assert legacy.getProxyHost().equals("my-proxy");
      assert legacy.getProxyPort().equals("8080");
      assert legacy.isSecure();
      assert legacy.isFetchPersistentState();
      assert legacy.getAsyncStoreConfig().isEnabled();
   }
}