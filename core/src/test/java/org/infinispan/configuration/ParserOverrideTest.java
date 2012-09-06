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
package org.infinispan.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.infinispan.test.TestingUtil.withCacheManager;



import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.ParserOverrideTest")
public class ParserOverrideTest {

   /**
    * This test makes sure that some named cached values are overridden properly
    */
   public void testNamedCacheOverride() throws Exception {
      final String cacheName = "asyncRepl";
      String xml1 = INFINISPAN_START_TAG +
            "   <namedCache name=\"" + cacheName + "\">\n" +
            "      <clustering mode=\"repl\">\n" +
            "         <stateTransfer fetchInMemoryState=\"false\"/>\n" +
            "         <async useReplQueue=\"false\" asyncMarshalling=\"false\"/>\n" +
            "      </clustering>\n" +
            "      <locking isolationLevel=\"REPEATABLE_READ\" concurrencyLevel=\"1000\" lockAcquisitionTimeout=\"20000\"/>\n" +
            "      <storeAsBinary enabled=\"true\"/>\n" +
            "      <expiration wakeUpInterval=\"23\" lifespan=\"50012\" maxIdle=\"1341\"/>\n" +
            "   </namedCache>\n" +
            TestingUtil.INFINISPAN_END_TAG;
      String xml2 = INFINISPAN_START_TAG +
            "   <namedCache name=\"" + cacheName + "\">\n" +
            "      <clustering mode=\"repl\">\n" +
            "         <stateTransfer fetchInMemoryState=\"true\"/>\n" +
            "         <sync replTimeout=\"30000\"/>\n" +
            "      </clustering>\n" +
            "      <locking isolationLevel=\"READ_COMMITTED\" concurrencyLevel=\"30\" lockAcquisitionTimeout=\"25000\"/>\n" +
            "      <storeAsBinary enabled=\"false\"/>\n" +
            "   </namedCache>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      ConfigurationBuilderHolder holder = TestCacheManagerFactory.buildAggregateHolder(xml1, xml2);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager(holder)){
         @Override
         public void call() {
            Configuration c = cm.getCacheConfiguration(cacheName);

            // These are all overridden values
            Assert.assertEquals(c.clustering().cacheMode(), CacheMode.REPL_SYNC);
            Assert.assertEquals(c.clustering().stateTransfer().fetchInMemoryState(), true);
            Assert.assertEquals(c.clustering().sync().replTimeout(), 30000);
            Assert.assertEquals(c.locking().isolationLevel(), IsolationLevel.READ_COMMITTED);
            Assert.assertEquals(c.locking().concurrencyLevel(), 30);
            Assert.assertEquals(c.locking().lockAcquisitionTimeout(), 25000);
            Assert.assertEquals(c.storeAsBinary().enabled(), false);

            // Following should have been taken over from first cache
            Assert.assertEquals(c.expiration().wakeUpInterval(), 23);
            Assert.assertEquals(c.expiration().lifespan(), 50012);
            Assert.assertEquals(c.expiration().maxIdle(), 1341);
         }
      });
   }

   /**
    * This test makes sure that both defaults are applied to a named cache
    */
   public void testDefaultCacheOverride() throws Exception {
      String xml1 = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <clustering mode=\"repl\">\n" +
            "         <stateTransfer fetchInMemoryState=\"false\"/>\n" +
            "         <async useReplQueue=\"false\" asyncMarshalling=\"false\"/>\n" +
            "      </clustering>\n" +
            "      <locking isolationLevel=\"REPEATABLE_READ\" concurrencyLevel=\"1000\" lockAcquisitionTimeout=\"20000\"/>\n" +
            "      <storeAsBinary enabled=\"true\"/>\n" +
            "      <expiration wakeUpInterval=\"23\" lifespan=\"50012\" maxIdle=\"1341\"/>\n" +
            "      <jmxStatistics enabled=\"true\"/>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;
      String xml2 = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <clustering mode=\"repl\">\n" +
            "         <stateTransfer fetchInMemoryState=\"true\"/>\n" +
            "         <sync replTimeout=\"30000\"/>\n" +
            "      </clustering>\n" +
            "      <locking isolationLevel=\"READ_COMMITTED\" concurrencyLevel=\"30\" lockAcquisitionTimeout=\"25000\"/>\n" +
            "      <storeAsBinary enabled=\"false\"/>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      ConfigurationBuilderHolder holder = TestCacheManagerFactory.buildAggregateHolder(xml1, xml2);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager(holder)){
         @Override
         public void call() {
            Configuration c = cm.getDefaultCacheConfiguration();

            // These are all overridden values
            Assert.assertEquals(c.clustering().cacheMode(), CacheMode.REPL_SYNC);
            Assert.assertEquals(c.clustering().stateTransfer().fetchInMemoryState(), true);
            Assert.assertEquals(c.clustering().sync().replTimeout(), 30000);
            Assert.assertEquals(c.locking().isolationLevel(), IsolationLevel.READ_COMMITTED);
            Assert.assertEquals(c.locking().concurrencyLevel(), 30);
            Assert.assertEquals(c.locking().lockAcquisitionTimeout(), 25000);
            Assert.assertEquals(c.storeAsBinary().enabled(), false);

            // Following should have been taken over from first cache
            Assert.assertEquals(c.expiration().wakeUpInterval(), 23);
            Assert.assertEquals(c.expiration().lifespan(), 50012);
            Assert.assertEquals(c.expiration().maxIdle(), 1341);
            Assert.assertEquals(c.jmxStatistics().enabled(), true);
         }
      });
   }

   /**
    * This test makes sure that both defaults are applied to a named cache then
    * named caches in order are applied to a named cache
    */
   public void testDefaultAndNamedCacheOverride() throws Exception {
      final String cacheName = "ourCache";
      String xml1 = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <clustering mode=\"repl\">\n" +
            "         <stateTransfer fetchInMemoryState=\"false\"/>\n" +
            "         <async useReplQueue=\"false\" asyncMarshalling=\"false\"/>\n" +
            "      </clustering>\n" +
            "      <locking isolationLevel=\"REPEATABLE_READ\" concurrencyLevel=\"1000\" lockAcquisitionTimeout=\"20000\"/>\n" +
            "      <storeAsBinary enabled=\"true\"/>\n" +
            "      <expiration wakeUpInterval=\"23\" lifespan=\"50012\" maxIdle=\"1341\"/>\n" +
            "      <deadlockDetection enabled=\"true\" spinDuration=\"1221\"/>\n" +
            "   </default>\n" +
            "   <namedCache name=\"" + cacheName + "\">\n" +
            "      <clustering>\n" +
            "         <async useReplQueue=\"true\" replQueueInterval=\"105\" replQueueMaxElements=\"341\"/>\n" +
            "      </clustering>\n" +
            "      <jmxStatistics enabled=\"true\"/>\n" +
            "      <deadlockDetection enabled=\"true\" spinDuration=\"502\"/>\n" +
            "      <deadlockDetection enabled=\"true\" spinDuration=\"1223\"/>\n" +
            "   </namedCache>" +
            TestingUtil.INFINISPAN_END_TAG;
      String xml2 = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <clustering mode=\"repl\">\n" +
            "         <stateTransfer fetchInMemoryState=\"true\"/>\n" +
            "         <sync replTimeout=\"30000\"/>\n" +
            "      </clustering>\n" +
            "      <locking isolationLevel=\"READ_COMMITTED\" concurrencyLevel=\"30\" lockAcquisitionTimeout=\"25000\"/>\n" +
            "      <storeAsBinary enabled=\"false\"/>\n" +
            "      <deadlockDetection enabled=\"true\" spinDuration=\"1222\"/>\n" +
            "   </default>\n" +
            "   <namedCache name=\"" + cacheName + "\">\n" +
            "      <clustering mode=\"dist\">\n" +
            "         <hash numOwners=\"3\" numSegments=\"51\"/>\n" +
            "         <l1 enabled=\"true\" lifespan=\"12345\"/>\n" +
            "         <async useReplQueue=\"false\"/>\n" +
            "      </clustering>\n" +
            "      <jmxStatistics enabled=\"true\"/>\n" +
            "      <deadlockDetection enabled=\"true\" spinDuration=\"1224\"/>\n" +
            "   </namedCache>" +
            TestingUtil.INFINISPAN_END_TAG;

      ConfigurationBuilderHolder holder = TestCacheManagerFactory.buildAggregateHolder(xml1, xml2);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager(holder)){
         @Override
         public void call() {
            Configuration c = cm.getCacheConfiguration(cacheName);

            // These are all overridden values
            Assert.assertEquals(c.clustering().cacheMode(), CacheMode.DIST_ASYNC);
            Assert.assertEquals(c.clustering().hash().numOwners(), 3);
            Assert.assertEquals(c.clustering().hash().numSegments(), 51);
            Assert.assertEquals(c.clustering().l1().enabled(), true);
            Assert.assertEquals(c.clustering().l1().lifespan(), 12345);
            Assert.assertEquals(c.clustering().stateTransfer().fetchInMemoryState(), true);
            Assert.assertEquals(c.clustering().async().useReplQueue(), false);
            Assert.assertEquals(c.clustering().async().replQueueInterval(), 105);
            Assert.assertEquals(c.clustering().async().replQueueMaxElements(), 341);
            Assert.assertEquals(c.jmxStatistics().enabled(), true);
            Assert.assertEquals(c.locking().isolationLevel(), IsolationLevel.READ_COMMITTED);
            Assert.assertEquals(c.locking().concurrencyLevel(), 30);
            Assert.assertEquals(c.locking().lockAcquisitionTimeout(), 25000);
            Assert.assertEquals(c.storeAsBinary().enabled(), false);
            Assert.assertEquals(c.expiration().wakeUpInterval(), 23);
            Assert.assertEquals(c.expiration().lifespan(), 50012);
            Assert.assertEquals(c.expiration().maxIdle(), 1341);
            Assert.assertEquals(c.deadlockDetection().enabled(), true);
            Assert.assertEquals(c.deadlockDetection().spinDuration(), 1224);
         }
      });
   }
}
