package org.infinispan.distribution;

import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.TimeoutException;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional")
@CleanupAfterMethod
public class RemoteGetFailureTest extends MultipleCacheManagersTest {
   private boolean staggered;
   private Object key;

   @Override
   public Object[] factory() {
      return new Object[] {
         new RemoteGetFailureTest().staggered(true),
         new RemoteGetFailureTest().staggered(false)
      };
   }

   @Override
   protected String parameters() {
      return "[staggered=" + staggered + "]";
   }

   protected RemoteGetFailureTest staggered(boolean staggered) {
      this.staggered = staggered;
      return this;
   }


   @Override
   protected void createCacheManagers() throws Throwable {
      setStaggered(staggered);
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      // cache stop takes quite long when the view splits
      builder.clustering().stateTransfer().timeout(10, TimeUnit.SECONDS);
      builder.clustering().remoteTimeout(5, TimeUnit.SECONDS);
      createCluster(builder, 3);
      waitForClusterToForm();
      key = getKeyForCache(cache(1), cache(2));
   }

   private static void setStaggered(boolean staggered) {
      try {
         Field staggerDelayNanos = CommandAwareRpcDispatcher.class.getDeclaredField("STAGGER_DELAY_NANOS");
         staggerDelayNanos.setAccessible(true);
         staggerDelayNanos.setLong(null, staggered ? TimeUnit.MILLISECONDS.toNanos(5) : 0L);
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }
   }

   @AfterClass
   @Override
   protected void destroy() {
      setStaggered(true); // the default
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      // When we send a ClearCommand from node that does not have a newer view installed to node that has already
      // installed a view without the sender, the message is dropped and the ClearCommand has to time out.
      // Therefore, don't issue the clear command at all.
      TestingUtil.killCacheManagers(false, cacheManagers.toArray(new EmbeddedCacheManager[cacheManagers.size()]));
      cacheManagers.clear();
   }

   public void testDelayed(Method m) {
      initAndCheck(m);

      CountDownLatch release = new CountDownLatch(1);
      cache(1).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(null, release), 0);

      long requestStart = System.nanoTime();
      assertEquals(m.getName(), cache(0).get(key));
      long requestEnd = System.nanoTime();
      long remoteTimeout = cache(0).getCacheConfiguration().clustering().remoteTimeout();
      long delay = TimeUnit.NANOSECONDS.toMillis(requestEnd - requestStart);
      assertTrue(delay < remoteTimeout);

      release.countDown();
   }

   public void testExceptionFromBothOwners(Method m) {
      initAndCheck(m);

      cache(1).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new FailingInterceptor(), 0);
      cache(2).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new FailingInterceptor(), 0);

      expectException(RemoteException.class, CacheException.class, "Injected", () -> cache(0).get(key));
   }

   public void testExceptionFromOneOwnerOtherTimeout(Method m) {
      initAndCheck(m);

      CountDownLatch release = new CountDownLatch(1);
      cache(1).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new FailingInterceptor(), 0);
      cache(2).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(null, release), 0);

      // It's not enough to test if the exception is TimeoutException as we want the remote get fail immediately
      // upon exception.

      // We cannot mock TimeService in ScheduledExecutor, so we have to measure if the response was fast
      // remoteTimeout is gracious enough (15s) to not cause false positives
      long requestStart = System.nanoTime();
      try {
         expectException(RemoteException.class, CacheException.class, "Injected", () -> cache(0).get(key));
         long exceptionThrown = System.nanoTime();
         long remoteTimeout = cache(0).getCacheConfiguration().clustering().remoteTimeout();
         long delay = TimeUnit.NANOSECONDS.toMillis(exceptionThrown - requestStart);
         assertTrue(delay < remoteTimeout);
      } finally {
         release.countDown();
      }
   }

   public void testBothOwnersSuspected(Method m) throws ExecutionException, InterruptedException {
      initAndCheck(m);

      CountDownLatch arrival = new CountDownLatch(2);
      CountDownLatch release = new CountDownLatch(1);
      AtomicInteger thrown = new AtomicInteger();
      AtomicInteger retried = new AtomicInteger();
      cache(0).getAdvancedCache().getAsyncInterceptorChain().addInterceptorAfter(new CheckOTEInterceptor(thrown, retried), StateTransferInterceptor.class);
      cache(1).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(arrival, release), 0);
      cache(2).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(arrival, release), 0);

      Future<Object> future = fork(() -> cache(0).get(key));
      assertTrue(arrival.await(10, TimeUnit.SECONDS));

      installNewView(cache(0), cache(0));

      // The entry was lost, so we'll get null
      assertNull(future.get());
      // Since we've lost all owners
      assertEquals(1, thrown.get()); // OwnersLostException
      assertEquals(0, retried.get());
      release.countDown();
   }

   public void testOneOwnerSuspected(Method m) throws ExecutionException, InterruptedException {
      initAndCheck(m);

      CountDownLatch arrival = new CountDownLatch(2);
      CountDownLatch release1 = new CountDownLatch(1);
      CountDownLatch release2 = new CountDownLatch(1);
      cache(1).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(arrival, release1), 0);
      cache(2).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(arrival, release2), 0);

      Future<?> future = fork(() -> {
         assertEquals(cache(0).get(key), m.getName());
      });
      assertTrue(arrival.await(10, TimeUnit.SECONDS));

      installNewView(cache(0), cache(0), cache(1));

      // suspection should not fail the operation
      assertFalse(future.isDone());
      release1.countDown();
      future.get();
      release2.countDown();
   }

   public void testOneOwnerSuspectedNoFilter(Method m) throws ExecutionException, InterruptedException {
      initAndCheck(m);

      CountDownLatch arrival = new CountDownLatch(2);
      CountDownLatch release1 = new CountDownLatch(1);
      CountDownLatch release2 = new CountDownLatch(1);
      cache(1).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(arrival, release1), 0);
      cache(2).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(arrival, release2), 0);

      Address address1 = address(1);
      Address address2 = address(2);
      List<Address> owners = Arrays.asList(address1, address2);

      ClusteredGetCommand clusteredGet = new ClusteredGetCommand(key, ByteString.fromString(cache(0).getName()), 0);
      final int timeout = 15;
      RpcOptions rpcOptions = new RpcOptions(timeout, TimeUnit.SECONDS, null, ResponseMode.WAIT_FOR_VALID_RESPONSE, DeliverOrder.NONE);

      RpcManager rpcManager = cache(0).getAdvancedCache().getRpcManager();
      CompletableFuture<Map<Address, Response>> future = rpcManager.invokeRemotelyAsync(owners, clusteredGet, rpcOptions);

      assertTrue(arrival.await(10, TimeUnit.SECONDS));

      installNewView(cache(0), cache(0), cache(1));

      // RequestCorrelator processes the view asynchronously, so we need to wait a bit for node 2 to be suspected
      Thread.sleep(100);

      // suspection should not fail the operation
      assertFalse(future.isDone());
      long requestAllowed = System.nanoTime();
      release1.countDown();
      Map<Address, Response> responses = future.get();
      long requestCompleted = System.nanoTime();
      long requestSeconds = TimeUnit.NANOSECONDS.toSeconds(requestCompleted - requestAllowed);

      assertTrue("Request took too long: " + requestSeconds, requestSeconds < timeout / 2);
      assertEquals(SuccessfulResponse.create(new ImmortalCacheValue(m.getName())), responses.get(address1));
      assertEquals(CacheNotFoundResponse.INSTANCE, responses.get(address2));
      release2.countDown();
   }

   public void testOneOwnerSuspectedOtherTimeout(Method m) throws ExecutionException, InterruptedException {
      initAndCheck(m);

      CountDownLatch arrival = new CountDownLatch(2);
      CountDownLatch release = new CountDownLatch(1);
      cache(1).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(arrival, release), 0);
      cache(2).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new DelayingInterceptor(arrival, release), 0);

      Future<?> future = fork(() -> {
         long start = System.nanoTime();
         Exceptions.expectException(TimeoutException.class, () -> cache(0).get(key));
         long end = System.nanoTime();
         long duration = TimeUnit.NANOSECONDS.toMillis(end - start);
         assertTrue("Request did not wait for long enough: " + duration,
               duration >= cache(0).getCacheConfiguration().clustering().remoteTimeout());
      });
      assertTrue(arrival.await(10, TimeUnit.SECONDS));

      installNewView(cache(0), cache(0), cache(1));

      // suspection should not fail the operation
      assertFalse(future.isDone());
      future.get();
      release.countDown();
   }

   private void initAndCheck(Method m) {
      cache(0).put(key, m.getName());
      assertEquals(m.getName(), cache(1).get(key));
      assertEquals(m.getName(), cache(2).get(key));
   }

   private void installNewView(Cache installing, Cache... cachesInView) {
      JGroupsTransport transport = (JGroupsTransport) installing.getCacheManager().getTransport();
      JChannel channel = transport.getChannel();

      org.jgroups.Address[] members = Stream.of(cachesInView)
                                            .map(c -> ((JGroupsAddress) address(c)).getJGroupsAddress())
                                            .toArray(org.jgroups.Address[]::new);
      View view = View.create(members[0], transport.getViewId() + 1, members);
      ((GMS) channel.getProtocolStack().findProtocol(GMS.class)).installView(view);
   }

   private static class FailingInterceptor extends DDAsyncInterceptor {
      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         throw new CacheException("Injected");
      }
   }

   private static class DelayingInterceptor extends DDAsyncInterceptor {
      private final CountDownLatch arrival;
      private final CountDownLatch release;

      private DelayingInterceptor(CountDownLatch arrival, CountDownLatch release) {
         this.arrival = arrival;
         this.release = release;
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         if (arrival != null) arrival.countDown();
         // the timeout has to be longer than remoteTimeout!
         release.await(30, TimeUnit.SECONDS);
         return super.visitGetCacheEntryCommand(ctx, command);
      }
   }

   private class CheckOTEInterceptor extends DDAsyncInterceptor {
      private final AtomicInteger thrown;
      private final AtomicInteger retried;

      public CheckOTEInterceptor(AtomicInteger thrown, AtomicInteger retried) {
         this.thrown = thrown;
         this.retried = retried;
      }

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
            retried.incrementAndGet();
         }
         return invokeNextAndExceptionally(ctx, command, (rCtx, rCommand, t) -> {
            thrown.incrementAndGet();
            throw t;
         });
      }
   }
}
