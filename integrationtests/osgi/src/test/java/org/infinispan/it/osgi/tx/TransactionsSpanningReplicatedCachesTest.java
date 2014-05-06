package org.infinispan.it.osgi.tx;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class TransactionsSpanningReplicatedCachesTest extends org.infinispan.tx.TransactionsSpanningReplicatedCachesTest {
   @Override
   protected void createCacheManagers() {
      //not used
   }

   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @Before
   public void setUp() {
      ConfigurationBuilder c = getConfiguration();
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      defineConfigurationOnAllManagers("c1", c);
      defineConfigurationOnAllManagers("c2", c);
   }

   @After
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManagers);
   }

   @Test
   public void testReadOnlyTransaction() throws Exception {
      super.testReadOnlyTransaction();
   }

   @Test
   public void testCommitSpanningCaches() throws Exception {
      super.testCommitSpanningCaches();
   }

   @Test
   public void testRollbackSpanningCaches() throws Exception {
      super.testRollbackSpanningCaches();
   }

   @Test
   public void testRollbackSpanningCaches2() throws Exception {
      super.testRollbackSpanningCaches2();
   }

   @Test
   public void testSimpleCommit() throws Exception {
      super.testSimpleCommit();
   }

   @Test
   public void testPutIfAbsent() throws Exception {
      super.testPutIfAbsent();
   }

   @Test
   public void testTwoNamedCachesSameNode() throws Exception {
      super.testTwoNamedCachesSameNode();
   }

   @Test
   public void testDefaultCacheAndNamedCacheSameNode() throws Exception {
      super.testDefaultCacheAndNamedCacheSameNode();
   }

   @Test
   public void testTwoNamedCachesDifferentNodes() throws Exception {
      super.testTwoNamedCachesDifferentNodes();
   }

   @Test
   public void testDefaultCacheAndNamedCacheDifferentNodes() throws Exception {
      super.testDefaultCacheAndNamedCacheDifferentNodes();
   }
}
