package org.infinispan.test.hibernate.cache.commons.functional.cluster;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.stat.Statistics;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Customer;
import org.infinispan.test.hibernate.cache.commons.util.ExpectingInterceptor;
import org.infinispan.test.hibernate.cache.commons.util.TestConfigurationHook;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.hibernate.cache.commons.util.TxUtil.withSession;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractPartialUpdateTest extends DualNodeTest {

   static final int NUM_NODES = 2;

   protected SessionFactoryImplementor localFactory;
   protected SessionFactoryImplementor remoteFactory;
   private AdvancedCache<?, ?> remoteCustomerCache;

   @Override
   public List<Object[]> getParameters() {
      return Collections.singletonList(READ_WRITE_REPLICATED);
   }

   @Override
   public void startUp() {
      super.startUp();
      localFactory = sessionFactory();
      remoteFactory = secondNodeEnvironment().getSessionFactory();
      remoteCustomerCache = ClusterAware
         .getCacheManager(DualNodeTest.REMOTE)
         .getCache(Customer.class.getName()).getAdvancedCache();
   }

   @Override
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(TestRegionFactory.CONFIGURATION_HOOK, getInjectPartialFailure());
   }

   abstract Class<? extends TestConfigurationHook> getInjectPartialFailure();

   public String getDbName() {
      return getClass().getName().replaceAll("\\W", "_");
   }

   @Test
   public void testPartialUpdate() throws Exception {
      Statistics statsNode0 = getStatistics(localFactory);
      withTxSession(localFactory, s -> {
         Customer customer = new Customer();
         customer.setName("JBoss");
         s.persist(customer);
      });

      assertEquals(1, statsNode0.getSecondLevelCachePutCount());
      assertEquals(0, statsNode0.getSecondLevelCacheMissCount());
      assertEquals(0, statsNode0.getSecondLevelCacheHitCount());

      // Wait for value to be applied remotely
      waitForRemotePut();

      Statistics statsNode1 = getStatistics(remoteFactory);
      withSession(remoteFactory.withOptions(), s -> {
         Customer customer = s.load(Customer.class, 1);
         assertEquals("JBoss", customer.getName());
      });

      assertEquals(0, statsNode1.getSecondLevelCachePutCount());
      assertEquals(0, statsNode1.getSecondLevelCacheMissCount());
      assertEquals(1, statsNode1.getSecondLevelCacheHitCount());

      final boolean updated = doUpdate();
      if (updated) {
         withSession(localFactory.withOptions(), s -> {
            Customer customer = s.load(Customer.class, 1);
            assertEquals("JBoss, a division of Red Hat", customer.getName());
         });

         withSession(remoteFactory.withOptions(), s -> {
            Customer customer = s.load(Customer.class, 1);
            assertEquals("JBoss, a division of Red Hat", customer.getName());
         });
      }
   }

   public void waitForRemotePut() throws InterruptedException {
      CountDownLatch remoteLatch = new CountDownLatch(1);
      ExpectingInterceptor.get(remoteCustomerCache)
         .when((ctx, cmd) -> cmd instanceof ReadWriteKeyCommand)
         .countDown(remoteLatch);

      assertTrue(remoteLatch.await(2, TimeUnit.SECONDS));
      ExpectingInterceptor.cleanup(remoteCustomerCache);
   }

   protected abstract boolean doUpdate() throws Exception;

   public Statistics getStatistics(SessionFactoryImplementor sessionFactory) {
      final Statistics stats = sessionFactory.getStatistics();
      stats.clear();
      return stats;
   }

   public static abstract class AbstractInjectPartialFailure extends TestConfigurationHook {

      static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(AbstractInjectPartialFailure.class);

      public AbstractInjectPartialFailure(Properties properties) {
         super(properties);
      }

      private static int ENTITY_CONFIG_COUNTER = 0;

      @Override
      public synchronized void amendCacheConfiguration(String cacheName, ConfigurationBuilder configurationBuilder) {
         super.amendCacheConfiguration(cacheName, configurationBuilder);
         log.tracef("Amend cache configuration for: %s", cacheName);
         if ("entity".equals(cacheName)) {
            if (ENTITY_CONFIG_COUNTER == NUM_NODES - 1) {
               configurationBuilder.customInterceptors().addInterceptor()
                  .interceptorClass(getFailureInducingInterceptorClass())
                  .position(InterceptorConfiguration.Position.FIRST);
               log.trace("Injecting FailureInducingInterceptor into " + cacheName);
            }

            ENTITY_CONFIG_COUNTER++;
         }
      }

      abstract Class<? extends AsyncInterceptor> getFailureInducingInterceptorClass();

   }

   public static class InducedException extends Exception {

      public InducedException(String message) {
         super(message);
      }

   }

}
