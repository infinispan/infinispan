/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.override;

import junit.framework.Assert;
import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests verifying that the overriding of the configuration which is read from the configuration XML file is done
 * properly and later operations behaviour is as expected.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "configuration.override.XMLConfigurationOverridingTest")
public class XMLConfigurationOverridingTest extends AbstractInfinispanTest implements Serializable {

   private static final String simpleCacheName = "simpleLocalCache";
   private static final String localCacheWithEviction = "localCacheWithEviction";
   private static final String replSync = "replSync";
   private static final String simpleNonTransactionalCache = "simpleNonTransactionalCache";
   private static final String simpleTransactionalCache = "simpleTransactionalCache";
   private static final String distCacheToChange = "distCacheToChange";
   private static final String replAsync = "replAsync";

   public void testLocalCacheOverrideWithEviction() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Assert.assertEquals(EvictionStrategy.NONE, cm.getCacheConfiguration(simpleCacheName).eviction().strategy());

            Configuration newConfig = new ConfigurationBuilder().eviction().strategy(EvictionStrategy.LRU)
                  .maxEntries(5).build();

            cm.defineConfiguration(simpleCacheName, newConfig);

            Assert.assertEquals(EvictionStrategy.LRU, cm.getCacheConfiguration(simpleCacheName).eviction().strategy());
            Assert.assertEquals(5, cm.getCacheConfiguration(simpleCacheName).eviction().maxEntries());

            for(int i = 0; i < 10; i++) {
               cm.getCache(simpleCacheName).put("test" + i, "value" + i);
            }

            Assert.assertTrue(cm.getCache(simpleCacheName).size() <= 5);
         }
      });
   }

   public void testLocalCacheOverrideWithConfiguredEviction() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Assert.assertEquals(EvictionStrategy.LRU, cm.getCacheConfiguration(localCacheWithEviction).eviction().strategy());
            Assert.assertEquals(10, cm.getCacheConfiguration(localCacheWithEviction).eviction().maxEntries());

            Configuration newConfig = new ConfigurationBuilder().eviction().strategy(EvictionStrategy.LIRS)
                  .maxEntries(20).build();

            cm.defineConfiguration(localCacheWithEviction, newConfig);

            Assert.assertEquals(EvictionStrategy.LIRS, cm.getCacheConfiguration(localCacheWithEviction).eviction().strategy());
            Assert.assertEquals(20, cm.getCacheConfiguration(localCacheWithEviction).eviction().maxEntries());

            for(int i = 0; i < 30; i++) {
               cm.getCache(localCacheWithEviction).put("test" + i, "value" + i);
            }

            Assert.assertTrue(cm.getCache(localCacheWithEviction).size() <= 20 && cm.getCache(localCacheWithEviction).size() > 10);
         }
      });
   }

   public void testLocalCacheOverrideWithConfiguredExpiration() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Assert.assertEquals(10000, cm.getCacheConfiguration(localCacheWithEviction).expiration().lifespan());
            Assert.assertEquals(60000, cm.getCacheConfiguration(localCacheWithEviction).expiration().wakeUpInterval());
            Assert.assertEquals(-1, cm.getCacheConfiguration(localCacheWithEviction).expiration().maxIdle());

            Configuration newConfig = new ConfigurationBuilder().expiration().lifespan(5000).wakeUpInterval(1000).maxIdle(2000).build();

            cm.defineConfiguration(localCacheWithEviction, newConfig);

            Assert.assertEquals(5000, cm.getCacheConfiguration(localCacheWithEviction).expiration().lifespan());
            Assert.assertEquals(2000, cm.getCacheConfiguration(localCacheWithEviction).expiration().maxIdle());
            Assert.assertEquals(1000, cm.getCacheConfiguration(localCacheWithEviction).expiration().wakeUpInterval());
         }
      });
   }

   public void testLocalCacheOverrideToClustered() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Assert.assertEquals(CacheMode.LOCAL, cm.getCacheConfiguration(simpleCacheName).clustering().cacheMode());

            Configuration newConfig = new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).build();
            cm.defineConfiguration(simpleCacheName, newConfig);

            Assert.assertEquals(CacheMode.REPL_SYNC, cm.getCacheConfiguration(simpleCacheName).clustering().cacheMode());

            for(int i = 0; i < 10; i++) {
               cm.getCache(simpleCacheName).put("test" + i, "value" + i);
            }

            try {
               final EmbeddedCacheManager cm1 = TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml");
               cm1.defineConfiguration(simpleCacheName, newConfig);

               eventually(new Condition() {

                  @Override
                  public boolean isSatisfied() throws Exception {
                     try {
                        return cm1.getCache(simpleCacheName).size() == cm.getCache(simpleCacheName).size();
                     } finally {
                        TestingUtil.killCacheManagers(cm1);
                     }
                  }
               });
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      });
   }

   public void testClusteredCacheOverrideToLocal() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Assert.assertEquals(CacheMode.REPL_SYNC, cm.getCacheConfiguration(replSync).clustering().cacheMode());

            Configuration newConfig = new ConfigurationBuilder().clustering().cacheMode(CacheMode.LOCAL).build();
            cm.defineConfiguration(replSync, newConfig);

            Assert.assertEquals(CacheMode.LOCAL, cm.getCacheConfiguration(replSync).clustering().cacheMode());

            for(int i = 0; i < 10; i++) {
               cm.getCache(replSync).put("test" + i, "value" + i);
            }

            EmbeddedCacheManager cm1 = null;
            try {
               cm1 = TestCacheManagerFactory.createClusteredCacheManager();

               cm1.defineConfiguration(replSync, newConfig);
               Assert.assertTrue(cm1.getCache(replSync).isEmpty());
            } finally {
               TestingUtil.killCacheManagers(cm1);
            }
         }
      });
   }

   public void testOverrideJmxStatistics() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Assert.assertFalse(cm.getCacheConfiguration(replSync).jmxStatistics().enabled());

            Configuration conf = new ConfigurationBuilder().jmxStatistics().enable().build();

            cm.defineConfiguration(replSync, conf);

            Assert.assertEquals(CacheMode.LOCAL, cm.getCacheConfiguration(replSync).clustering().cacheMode());
            Assert.assertTrue(cm.getCacheConfiguration(replSync).jmxStatistics().enabled());
         }
      });
   }

   public void testOverrideLoaders() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration configuration = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertFalse(configuration.jmxStatistics().enabled());

            Configuration conf = new ConfigurationBuilder().eviction().maxEntries(5).strategy(EvictionStrategy.LRU)
                  .loaders().passivation(true).addFileCacheStore().fsyncInterval(10000)
                  .fsyncMode(FileCacheStoreConfigurationBuilder.FsyncMode.DEFAULT).location(".").build();

            cm.defineConfiguration(simpleCacheName, conf);

            configuration = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertEquals(CacheMode.LOCAL, configuration.clustering().cacheMode());
            Assert.assertTrue(configuration.loaders().passivation());

            Cache cache = cm.getCache(simpleCacheName);
            for (int i = 0; i < 10; i++) {
               cache.put("key" + i, "value" + i);
            }

            Assert.assertTrue(cache.size() <= 5);
            for (int i = 0; i < 10; i++) {
               Assert.assertNotNull(cache.get("key" + i));
            }
         }
      });
   }

   public void testOverrideIndexing() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertFalse(cnf.indexing().enabled());

            Configuration conf = new ConfigurationBuilder().indexing().enable().indexLocalOnly(false)
                  .addProperty("default.directory_provider", "infinispan").build();

            cm.defineConfiguration(simpleCacheName, conf);

            cnf = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertTrue(cnf.indexing().enabled());
            Assert.assertFalse(cnf.indexing().indexLocalOnly());
            Assert.assertEquals("infinispan", cnf.indexing().properties().getProperty("default.directory_provider"));
            Assert.assertFalse(cm.getCacheNames().contains("LuceneIndexesMetadata"));

            for (int i = 0; i < 10; i++) {
               cm.getCache(simpleCacheName + 1).put("key" + i, new NonIndexedClass("value" + i));
            }

            Assert.assertFalse(cm.getCacheNames().contains("LuceneIndexesMetadata"));
         }
      });
   }

   public void testOverrideNonTransactional2Transactional() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(simpleNonTransactionalCache);
            Assert.assertEquals(TransactionMode.NON_TRANSACTIONAL, cnf.transaction().transactionMode());
            Assert.assertEquals(100, cnf.locking().concurrencyLevel());

            Configuration conf = new ConfigurationBuilder().transaction().transactionMode(TransactionMode.TRANSACTIONAL)
                  .jmxStatistics().enable().locking().concurrencyLevel(1).build();

            cm.defineConfiguration(simpleNonTransactionalCache, conf);

            cnf = cm.getCacheConfiguration(simpleNonTransactionalCache);
            Assert.assertEquals(TransactionMode.TRANSACTIONAL, cnf.transaction().transactionMode());
            Assert.assertTrue(cnf.jmxStatistics().enabled());
            Assert.assertEquals(1, cnf.locking().concurrencyLevel());

            TransactionManager tm = cm.getCache(simpleNonTransactionalCache).getAdvancedCache().getTransactionManager();

            try {
               tm.begin();
               for (int i = 0; i < 10; i++) {
                  cm.getCache(simpleNonTransactionalCache).put("key" + i, "value" + i);
               }

               tm.commit();

               for (int i = 0; i < 10; i++) {
                  Assert.assertNotNull(cm.getCache(simpleNonTransactionalCache).get("key" + i));
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      });
   }

   public void testOverrideTransactional2NonTransactional() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(simpleTransactionalCache);
            Assert.assertEquals(TransactionMode.TRANSACTIONAL, cnf.transaction().transactionMode());

            Configuration conf = new ConfigurationBuilder().transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
                  .build();

            cm.defineConfiguration(simpleTransactionalCache, conf);

            cnf = cm.getCacheConfiguration(simpleTransactionalCache);
            Assert.assertEquals(TransactionMode.NON_TRANSACTIONAL, cnf.transaction().transactionMode());

            TransactionManager tm = cm.getCache(simpleTransactionalCache).getAdvancedCache().getTransactionManager();
            Assert.assertNull(tm);
            for (int i = 0; i < 10; i++) {
               cm.getCache(simpleTransactionalCache).put("key" + i, "value" + i);
            }

            for (int i = 0; i < 10; i++) {
               Assert.assertNotNull(cm.getCache(simpleTransactionalCache).get("key" + i));
            }
         }
      });
   }

   public void testOverrideStoreAsBinary() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertFalse(cnf.storeAsBinary().enabled());

            Configuration conf = new ConfigurationBuilder().storeAsBinary().enable().defensive(true).build();

            cm.defineConfiguration(simpleCacheName, conf);

            cnf = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertTrue(cnf.storeAsBinary().enabled());
            Assert.assertTrue(cnf.storeAsBinary().storeValuesAsBinary());

            List<NonIndexedClass> instances = new ArrayList<NonIndexedClass>();
            for (int i = 0; i < 10; i++) {
               NonIndexedClass cl = new NonIndexedClass("value" + i);
               instances.add(cl);
               cm.getCache(simpleCacheName).put("key" + i, cl);
            }

            //Changing the data in referenced variables
            for(NonIndexedClass indexedClass : instances) {
               indexedClass.description = "other value";
            }

            //Verifying that the entry in the cache is not changed.
            for (int i = 0; i < 10; i++) {
               Assert.assertEquals(new NonIndexedClass("value" + i), cm.getCache(simpleCacheName).get("key" + i));
            }
         }
      });
   }

   public void testOverrideClusteringNumOwners() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(distCacheToChange);
            Assert.assertEquals(CacheMode.DIST_SYNC, cnf.clustering().cacheMode());
            Assert.assertEquals(2, cnf.clustering().hash().numOwners());

            Configuration conf = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(1).build();

            cm.defineConfiguration(distCacheToChange, conf);

            cnf = cm.getCacheConfiguration(distCacheToChange);
            Assert.assertEquals(CacheMode.DIST_SYNC, cnf.clustering().cacheMode());
            Assert.assertEquals(1, cnf.clustering().hash().numOwners());

            Cache cache1 = cm.getCache(distCacheToChange);
            for (int i = 0; i < 10; i++) {
               cache1.put("key" + i, "value" + i);
            }

            EmbeddedCacheManager cm1 = null;
            try {
               cm1 = TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml");
               cm1.defineConfiguration(distCacheToChange, conf);

               Cache cache2 = cm1.getCache(distCacheToChange);
               waitForRehashToComplete(cache1, cache2);

               Assert.assertTrue(cache2.size() > 0 && cache2.size() != cache1.size());
            } catch (Exception e) {
               e.printStackTrace();
            } finally {
               TestingUtil.killCacheManagers(cm1);
            }
         }
      });
   }

   public void testOverrideClusteringSync2Async() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(distCacheToChange);
            Assert.assertEquals(CacheMode.DIST_SYNC, cnf.clustering().cacheMode());
            Assert.assertEquals(2, cnf.clustering().hash().numOwners());

            Configuration conf = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_ASYNC).build();

            cm.defineConfiguration(distCacheToChange, conf);

            cnf = cm.getCacheConfiguration(distCacheToChange);
            Assert.assertEquals(CacheMode.DIST_ASYNC, cnf.clustering().cacheMode());

            EmbeddedCacheManager cm1 = null;
            try {
               cm1 = TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml");
               cm1.defineConfiguration(distCacheToChange, conf);

               Cache cache1 = cm.getCache(distCacheToChange);
               Cache cache2 = cm1.getCache(distCacheToChange);

               ReplListener replList2 = new ReplListener(cache2, true, true);

               String key = "key1";
               String value = "value1";

               replList2.expect(PutKeyValueCommand.class);
               cache1.put(key, value);

               // allow for replication
               replList2.waitForRpc();
               assertEquals(value, cache1.get(key));
               assertEquals(value, cache2.get(key));
            } catch (Exception e) {
               e.printStackTrace();
            } finally {
               TestingUtil.killCacheManagers(cm1);
            }
         }
      });
   }

   public void testOverrideClusteringAsync2Sync() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(replAsync);
            Assert.assertEquals(CacheMode.REPL_ASYNC, cnf.clustering().cacheMode());

            Configuration conf = new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).build();

            cm.defineConfiguration(replAsync, conf);

            cnf = cm.getCacheConfiguration(replAsync);
            Assert.assertEquals(CacheMode.REPL_SYNC, cnf.clustering().cacheMode());

            EmbeddedCacheManager cm1 = null;
            try {
               cm1 = TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml");
               cm1.defineConfiguration(replAsync, conf);

               Cache cache1 = cm.getCache(replAsync);
               Cache cache2 = cm1.getCache(replAsync);

               String key = "key1";
               String value = "value1";

               cache1.put(key, value);
               assertEquals(value, cache1.get(key));
               assertEquals(value, cache2.get(key));
            } catch (Exception e) {
               e.printStackTrace();
            } finally {
               TestingUtil.killCacheManagers(cm1);
            }
         }
      });
   }

   public void testOverrideAddInterceptors() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertTrue(cnf.customInterceptors().interceptors().isEmpty());

            SimpleInterceptor interceptor = new SimpleInterceptor();
            Configuration conf = new ConfigurationBuilder().customInterceptors().addInterceptor().interceptor(interceptor)
                  .position(InterceptorConfiguration.Position.FIRST).build();

            cm.defineConfiguration(simpleCacheName, conf);

            cnf = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertFalse(cnf.customInterceptors().interceptors().isEmpty());

            Assert.assertFalse(interceptor.putOkay);
            cm.getCache(simpleCacheName).put("key1", "value1");

            Assert.assertTrue(interceptor.putOkay);
            Assert.assertEquals("value1", cm.getCache(simpleCacheName).get("key1"));
         }
      });
   }

   class NonIndexedClass implements Serializable {
      public String description;

      NonIndexedClass(String description) {
         this.description = description;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         NonIndexedClass that = (NonIndexedClass) o;

         if (!description.equals(that.description)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return description.hashCode();
      }
   }

   class SimpleInterceptor extends CommandInterceptor {
      private boolean putOkay;

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (isRightType(ctx)) putOkay = true;
         return super.visitPutKeyValueCommand(ctx, command);
      }

      private boolean isRightType(InvocationContext ctx) {
         return ctx instanceof SingleKeyNonTxInvocationContext;
      }
   }
}
