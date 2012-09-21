/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.remote.configuration.as;

import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.IOException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.remote.configuration.RemoteCacheStoreConfiguration;
import org.infinispan.loaders.remote.configuration.RemoteServerConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.remote.configuration.as.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   public void testStandaloneXml() throws IOException {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/as7/rcs-standalone.xml", true)) {

         @Override
         public void call() {
            Configuration c = cm.getCacheConfiguration("default");
            assert c.clustering().cacheMode().equals(CacheMode.LOCAL);
            assert c.locking().isolationLevel().equals(IsolationLevel.NONE);
            assert c.locking().lockAcquisitionTimeout() == 30000;
            assert c.locking().concurrencyLevel() == 1000;
            assert !c.locking().useLockStriping();
            assert c.transaction().transactionMode().equals(TransactionMode.NON_TRANSACTIONAL);
            assert c.eviction().strategy().equals(EvictionStrategy.LRU);
            assert c.eviction().maxEntries() == 1000;
            assert c.loaders().passivation();
            assert !c.loaders().fetchPersistentState();
            assert !c.loaders().shared();
            assert c.loaders().cacheLoaders().size() == 1;
            RemoteCacheStoreConfiguration rcsc = (RemoteCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);
            assert !rcsc.purgeOnStartup();
            assert rcsc.servers().size() == 1;
            RemoteServerConfiguration server = rcsc.servers().get(0);
            assert server.host().equals("remote-host");
            assert server.port() == 11222;
            assert rcsc.async().enabled();
            assert rcsc.async().flushLockTimeout() == 1;
            assert rcsc.async().modificationQueueSize() == 1024;
            assert rcsc.async().shutdownTimeout() == 25000;
            assert rcsc.async().threadPoolSize() == 1;
         }

      });

   }

}