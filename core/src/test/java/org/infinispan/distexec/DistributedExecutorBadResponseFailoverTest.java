package org.infinispan.distexec;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests dist.exec failover due to CacheNotFoundResponse or any other non SuccessfulResponse response.
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 */
@Test(groups = "functional", testName = "distexec.DistributedExecutorBadResponseFailoverTest")
public class DistributedExecutorBadResponseFailoverTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      addClusterEnabledCacheManager(builder);

      // this is the node sending DistributedExecuteCommand
      final EmbeddedCacheManager cacheManager1 = manager(0);
      TestingUtil.wrapGlobalComponent(cacheManager1, Transport.class,
            new TestingUtil.WrapFactory<Transport, Transport, CacheContainer>() {

               @Override
               public Transport wrap(CacheContainer wrapOn, Transport current) {
                  return new CacheNotFoundResponseTransport(current);
               }
            }, true);

      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      waitForClusterToForm(cacheName());
   }

   protected String cacheName() {
      return "DistributedExecutorBadResponseFailoverTest";
   }

   public void testBasicTargetRemoteDistributedCallable() throws Exception {
      long taskTimeout = TimeUnit.SECONDS.toMillis(15);
      EmbeddedCacheManager cacheManager1 = manager(0);
      final EmbeddedCacheManager cacheManager2 = manager(1);

      Cache<Object, Object> cache1 = cacheManager1.getCache();
      Cache<Object, Object> cache2 = cacheManager2.getCache();
      DistributedExecutorService des = null;

      try {
         des = new DefaultExecutorService(cache1);
         Address target = cache2.getAdvancedCache().getRpcManager().getAddress();

         DistributedTaskBuilder<Integer> builder = des.createDistributedTaskBuilder(new SimpleCallable())
               .failoverPolicy(DefaultExecutorService.RANDOM_NODE_FAILOVER)
               .timeout(taskTimeout, TimeUnit.MILLISECONDS);

         Future<Integer> future = des.submit(target, builder.build());
         AssertJUnit.assertEquals((Integer) 1, future.get());
      } catch (Exception ex) {
         AssertJUnit.fail("Task did not failover properly " + ex);
      } finally {
         des.shutdown();
      }
   }

   static class SimpleCallable implements Callable<Integer>, Serializable {

      /**
       *
       */
      private static final long serialVersionUID = -3130274337449595197L;

      public SimpleCallable() {
      }

      @Override
      public Integer call() throws Exception {
         return 1;
      }
   }

   private static class CacheNotFoundResponseTransport extends AbstractDelegatingTransport {

      public CacheNotFoundResponseTransport(Transport actual) {
         super(actual);
      }

      @Override
      public void start() {
         // Do not start the transport a second time
      }

      @Override
      public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
            ResponseMode mode, long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast)
            throws Exception {

         Map<Address, Response> properResponse = super.invokeRemotely(recipients, rpcCommand, mode, timeout,
               responseFilter, deliverOrder, anycast);

         // intercept the response we received for DistributedExecuteCommand
         if (rpcCommand instanceof DistributedExecuteCommand) {
            Map<Address, Response> cacheNotFoundResponse = new HashMap<Address, Response>();
            for (Entry<Address, Response> e : properResponse.entrySet()) {
               // and augment it to return CacheNotFoundResponse (any other non SuccessfulResponse will do)
               cacheNotFoundResponse.put(e.getKey(), CacheNotFoundResponse.INSTANCE);
            }
            return cacheNotFoundResponse;
         } else {
            return properResponse;
         }
      }
   }
}