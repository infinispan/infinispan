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
package org.infinispan.loaders.jdbc.configuration.as;

import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.IOException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedCacheStoreConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.jdbc.configuration.as.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   public void testStandaloneXml() throws IOException {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/as7/jdbc-standalone.xml", true)) {

         @Override
         public void call() {
            Configuration c = cm.getCacheConfiguration("default");
            assert c.clustering().cacheMode().equals(CacheMode.DIST_SYNC);
            assert c.locking().isolationLevel().equals(IsolationLevel.READ_COMMITTED);
            assert c.locking().lockAcquisitionTimeout() == 30000;
            assert c.locking().concurrencyLevel() == 1000;
            assert !c.locking().useLockStriping();
            assert c.transaction().transactionMode().equals(TransactionMode.NON_TRANSACTIONAL);
            assert c.eviction().strategy().equals(EvictionStrategy.LRU);
            assert c.eviction().maxEntries() == 1000;
            assert !c.loaders().passivation();
            assert !c.loaders().fetchPersistentState();
            assert !c.loaders().shared();
            assert c.loaders().cacheLoaders().size() == 1;
            JdbcMixedCacheStoreConfiguration jmcs = (JdbcMixedCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);
            assert jmcs.binaryTable().tableNamePrefix().equals("JDG_MIX_BKT");
            assert jmcs.binaryTable().idColumnName().equals("id");
            assert jmcs.binaryTable().idColumnType().equals("VARCHAR");
            assert jmcs.binaryTable().dataColumnName().equals("datum");
            assert jmcs.binaryTable().dataColumnType().equals("BINARY");
            assert jmcs.binaryTable().timestampColumnName().equals("version");
            assert jmcs.binaryTable().timestampColumnType().equals("BIGINT");
            assert jmcs.stringTable().tableNamePrefix().equals("JDG_MIX_STR");
            assert jmcs.stringTable().idColumnName().equals("id");
            assert jmcs.stringTable().idColumnType().equals("VARCHAR");
            assert jmcs.stringTable().dataColumnName().equals("datum");
            assert jmcs.stringTable().dataColumnType().equals("BINARY");
            assert jmcs.stringTable().timestampColumnName().equals("version");
            assert jmcs.stringTable().timestampColumnType().equals("BIGINT");

            c = cm.getCacheConfiguration("stringCache");
            assert c.clustering().cacheMode().equals(CacheMode.DIST_SYNC);
            assert c.loaders().passivation();
            assert !c.loaders().fetchPersistentState();
            assert !c.loaders().shared();
            assert c.loaders().cacheLoaders().size() == 1;
            JdbcStringBasedCacheStoreConfiguration jscs = (JdbcStringBasedCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);
            assert jscs.table().tableNamePrefix().equals("JDG_MC_SK");
            assert jscs.table().idColumnName().equals("id");
            assert jscs.table().idColumnType().equals("VARCHAR");
            assert jscs.table().dataColumnName().equals("datum");
            assert jscs.table().dataColumnType().equals("BINARY");
            assert jscs.table().timestampColumnName().equals("version");
            assert jscs.table().timestampColumnType().equals("BIGINT");
            assert jscs.async().enabled();
            assert jscs.async().flushLockTimeout() == 1;
            assert jscs.async().modificationQueueSize() == 1024;
            assert jscs.async().shutdownTimeout() == 25000;
            assert jscs.async().threadPoolSize() == 1;

            c = cm.getCacheConfiguration("binaryCache");
            assert c.clustering().cacheMode().equals(CacheMode.DIST_SYNC);
            assert !c.loaders().passivation();
            assert !c.loaders().fetchPersistentState();
            assert !c.loaders().shared();
            assert c.loaders().cacheLoaders().size() == 1;
            JdbcBinaryCacheStoreConfiguration jbcs = (JdbcBinaryCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);
            assert jbcs.table().tableNamePrefix().equals("JDG_NC_BK");
            assert jbcs.table().idColumnName().equals("id");
            assert jbcs.table().idColumnType().equals("VARCHAR");
            assert jbcs.table().dataColumnName().equals("datum");
            assert jbcs.table().dataColumnType().equals("BINARY");
            assert jbcs.table().timestampColumnName().equals("version");
            assert jbcs.table().timestampColumnType().equals("BIGINT");
         }

      });

   }

}