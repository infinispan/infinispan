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
package org.infinispan.configuration.as;

import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.IOException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.FileCacheStoreConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.as.ASXmlFileParsingTest")
public class ASXmlFileParsingTest extends AbstractInfinispanTest {

   public void testNamedCacheFile() throws IOException {

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/as7/standalone.xml")) {

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
            assert c.expiration().lifespan() == 2000;
            assert c.expiration().maxIdle() == 1000;
            assert c.loaders().passivation();
            assert !c.loaders().shared();
            assert c.loaders().cacheLoaders().size() == 1;
            FileCacheStoreConfiguration fcsc = (FileCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);
            assert fcsc.purgeOnStartup();
            assert fcsc.location().equals("nc");
            assert fcsc.async().enabled();
            assert fcsc.async().flushLockTimeout() == 1;
            assert fcsc.async().modificationQueueSize() == 1024;
            assert fcsc.async().shutdownTimeout() == 25000;
            assert fcsc.async().threadPoolSize() == 1;

            c = cm.getCacheConfiguration("distsync");
            assert c.clustering().cacheMode().equals(CacheMode.DIST_SYNC);
            assert c.locking().isolationLevel().equals(IsolationLevel.READ_COMMITTED);
            assert c.locking().lockAcquisitionTimeout() == 20000;
            assert c.locking().concurrencyLevel() == 500;
            assert c.locking().useLockStriping();
            assert c.transaction().recovery().enabled();
            assert c.eviction().strategy().equals(EvictionStrategy.LIRS);
            assert c.eviction().maxEntries() == 1000;
         }

      });

   }

}