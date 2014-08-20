package org.infinispan.configuration;

import static org.infinispan.test.TestingUtil.JGROUPS_CONFIG;
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
      String xml1 = INFINISPAN_START_TAG + JGROUPS_CONFIG +
            "<cache-container name=\"1\" default-cache=\"" + cacheName + "\">" +
            "   <replicated-cache name=\"" + cacheName + "\" mode=\"ASYNC\" async-marshalling=\"false\">\n" +
            "      <state-transfer enabled=\"false\"/>\n" +
            "      <locking isolation=\"REPEATABLE_READ\" concurrency-level=\"1000\" acquire-timeout=\"20000\"/>\n" +
            "      <store-as-binary/>\n" +
            "      <expiration interval=\"23\" lifespan=\"50012\" max-idle=\"1341\"/>\n" +
            "   </replicated-cache>\n" +
            "</cache-container>" +
            TestingUtil.INFINISPAN_END_TAG;
      String xml2 = INFINISPAN_START_TAG + JGROUPS_CONFIG +
            "<cache-container name=\"2\" default-cache=\"" + cacheName + "\">" +
            "   <replicated-cache name=\"" + cacheName + "\" mode=\"SYNC\" remote-timeout=\"30000\">\n" +
            "      <state-transfer enabled=\"true\"/>\n" +
            "      <locking isolation=\"READ_COMMITTED\" concurrency-level=\"30\" acquire-timeout=\"25000\"/>\n" +
            "      <store-as-binary keys=\"false\" values=\"false\"/>\n" +
            "   </replicated-cache>\n" +
            "</cache-container>" +
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
      String xml1 = INFINISPAN_START_TAG + JGROUPS_CONFIG +
            "<cache-container name=\"1\" default-cache=\"default-cache\">" +
            "   <replicated-cache name=\"default-cache\" mode=\"ASYNC\" statistics=\"true\">\n" +
            "      <state-transfer enabled=\"false\"/>\n" +
            "      <locking isolation=\"REPEATABLE_READ\" concurrency-level=\"1000\" acquire-timeout=\"20000\"/>\n" +
            "      <store-as-binary/>\n" +
            "      <expiration interval=\"23\" lifespan=\"50012\" max-idle=\"1341\"/>\n" +
            "   </replicated-cache>\n" +
            "</cache-container>" +
            TestingUtil.INFINISPAN_END_TAG;
      String xml2 = INFINISPAN_START_TAG + JGROUPS_CONFIG +
            "<cache-container name=\"2\" default-cache=\"default-cache\">" +
            "   <replicated-cache name=\"default-cache\" mode=\"SYNC\" remote-timeout=\"30000\">\n" +
            "      <state-transfer enabled=\"true\"/>\n" +
            "      <locking isolation=\"READ_COMMITTED\" concurrency-level=\"30\" acquire-timeout=\"25000\"/>\n" +
            "      <store-as-binary keys=\"false\" values=\"false\"/>\n" +
            "   </replicated-cache>\n" +
            "</cache-container>" +
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
      String xml1 = INFINISPAN_START_TAG + JGROUPS_CONFIG +
            "<cache-container name=\"1\" default-cache=\"default-cache\">" +
            "   <replicated-cache name=\"default-cache\" mode=\"ASYNC\" statistics=\"true\" deadlock-detection-spin=\"1221\">\n" +
            "      <state-transfer enabled=\"false\"/>\n" +
            "      <locking isolation=\"REPEATABLE_READ\" concurrency-level=\"1000\" acquire-timeout=\"20000\"/>\n" +
            "      <store-as-binary/>\n" +
            "      <expiration interval=\"23\" lifespan=\"50012\" max-idle=\"1341\"/>\n" +
            "   </replicated-cache>\n" +
            "   <replicated-cache name=\"" + cacheName + "\" mode=\"ASYNC\" queue-flush-interval=\"105\" queue-size=\"341\" statistics=\"true\" deadlock-detection-spin=\"1223\" />\n" +
            "</cache-container>" +
            TestingUtil.INFINISPAN_END_TAG;
      String xml2 = INFINISPAN_START_TAG + JGROUPS_CONFIG +
            "<cache-container name=\"2\" default-cache=\"default-cache\">" +
            "   <replicated-cache name=\"default-cache\" mode=\"SYNC\" deadlock-detection-spin=\"1222\" remote-timeout=\"30000\">\n" +
            "      <state-transfer enabled=\"true\"/>\n" +
            "      <locking isolation=\"READ_COMMITTED\" concurrency-level=\"30\" acquire-timeout=\"25000\"/>\n" +
            "      <store-as-binary keys=\"false\" values=\"false\"/>\n" +
            "   </replicated-cache>\n" +
            "   <distributed-cache name=\"" + cacheName + "\" mode=\"ASYNC\" owners=\"3\" segments=\"51\" l1-lifespan=\"12345\" queue-size=\"-1\" statistics=\"true\" deadlock-detection-spin=\"1224\" >\n" +
            "   </distributed-cache>" +
            "</cache-container>" +
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
            // Interval and max elements irrelevant since replication queue is disabled
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
