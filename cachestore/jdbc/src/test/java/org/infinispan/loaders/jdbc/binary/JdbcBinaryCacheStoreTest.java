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
package org.infinispan.loaders.jdbc.binary;

import static org.mockito.Mockito.*;

import org.infinispan.CacheImpl;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

import java.io.Serializable;

/**
 * Tester class for {@link JdbcBinaryCacheStore}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.binary.JdbcBinaryCacheStoreTest")
public class JdbcBinaryCacheStoreTest extends BaseCacheStoreTest {

   @Override
   protected CacheStore createCacheStore() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcBinaryCacheStoreConfig config = new JdbcBinaryCacheStoreConfig(connectionFactoryConfig, tm);
      JdbcBinaryCacheStore jdbcBucketCacheStore = new JdbcBinaryCacheStore();
      jdbcBucketCacheStore.init(config, new CacheImpl("aName"), getMarshaller());
      jdbcBucketCacheStore.start();
      assert jdbcBucketCacheStore.getConnectionFactory() != null;
      return jdbcBucketCacheStore;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      JdbcBinaryCacheStore jdbcBucketCacheStore = new JdbcBinaryCacheStore();
      JdbcBinaryCacheStoreConfig config = new JdbcBinaryCacheStoreConfig(false);
      config.setCreateTableOnStart(false);
      jdbcBucketCacheStore.init(config, new CacheImpl("aName"), new TestObjectStreamMarshaller());
      jdbcBucketCacheStore.start();
      assert jdbcBucketCacheStore.getConnectionFactory() == null;

      /* this will make sure that if a method like stop is called on the connection then it will barf an exception */
      ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
      TableManipulation tableManipulation = mock(TableManipulation.class);
      config.setTableManipulation(tableManipulation);

      tableManipulation.start(connectionFactory);
      tableManipulation.setCacheName("aName");
      jdbcBucketCacheStore.doConnectionFactoryInitialization(connectionFactory);

      //stop should be called even if this is an externally managed connection
      tableManipulation.stop();
      jdbcBucketCacheStore.stop();
   }

   public void testPurgeExpiredAllCodepaths() throws CacheLoaderException {
      FixedHashKey k1 = new FixedHashKey(1, "a");
      FixedHashKey k2 = new FixedHashKey(1, "b");
      cs.store(TestInternalCacheEntryFactory.create(k1, "value"));
      cs.store(TestInternalCacheEntryFactory.create(k2, "value", 60000)); // will expire
      for (int i = 0; i < 120; i++) {
         cs.store(TestInternalCacheEntryFactory.create(new FixedHashKey(i + 10, "non-exp k" + i), "value"));
         cs.store(TestInternalCacheEntryFactory.create(new FixedHashKey(i + 10, "exp k" + i), "value", 60000)); // will expire
      }
      assert cs.containsKey(k1);
      assert cs.containsKey(k2);
      TestingUtil.sleepThread(62000);
      cs.purgeExpired();
      assert cs.containsKey(k1);
      assert !cs.containsKey(k2);
   }

   private static final class FixedHashKey implements Serializable {
      String s;
      int i;

      private FixedHashKey(int i, String s) {
         this.s = s;
         this.i = i;
      }

      @Override
      public int hashCode() {
         return i;
      }

      @Override
      public boolean equals(Object other) {
         return other instanceof FixedHashKey && s.equals(((FixedHashKey) other).s);
      }
   }

}
