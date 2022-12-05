package org.infinispan.remoting.inboundhandler;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.testng.AssertJUnit.assertTrue;

@Scope(Scopes.NAMED_CACHE)
public class BlockingPerCacheInboundInvocationHandler implements PerCacheInboundInvocationHandler {

   private final Address address;
   private final PerCacheInboundInvocationHandler delegate;
   private final List<HandlerImpl<ReplicableCommand>> replicableCmdBlockTest = new CopyOnWriteArrayList<>();

   public static BlockingPerCacheInboundInvocationHandler replace(Cache<?, ?> cache) {
      return wrapInboundInvocationHandler(cache,
            iih -> {
               if (iih instanceof BlockingPerCacheInboundInvocationHandler) {
                  return (BlockingPerCacheInboundInvocationHandler) iih;
               }
               return new BlockingPerCacheInboundInvocationHandler(iih, cache.getCacheManager().getAddress());
            });
   }

   private BlockingPerCacheInboundInvocationHandler(PerCacheInboundInvocationHandler delegate, Address address) {
      this.delegate = delegate;
      this.address = address;
   }

   @Override
   public String toString() {
      return "PerCacheInboundInvocationHandler@" + address;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      for (HandlerImpl<ReplicableCommand> handler : replicableCmdBlockTest) {
         if (handler.test(command)) {
            handler.runAfterBlocked(() -> delegate.handle(command, reply, order));
            return;
         }
      }
      delegate.handle(command, reply, order);
   }

   @Override
   public void setFirstTopologyAsMember(int firstTopologyAsMember) {
      delegate.setFirstTopologyAsMember(firstTopologyAsMember);
   }

   @Override
   public int getFirstTopologyAsMember() {
      return delegate.getFirstTopologyAsMember();
   }

   @Override
   public void checkForReadyTasks() {
      delegate.checkForReadyTasks();
   }

   public BlockHandler blockRpcBefore(Predicate<ReplicableCommand> predicate, String timeoutMessage) {
      HandlerImpl<ReplicableCommand> handler = new HandlerImpl<>(predicate, timeoutMessage);
      replicableCmdBlockTest.add(handler);
      return handler;
   }

   public <T extends ReplicableCommand> BlockHandler blockRpcBefore(Class<T> commandToBlock) {
      return blockRpcBefore(commandToBlock::isInstance, commandToBlock.getSimpleName());
   }

   public void stopBlocking() {
      replicableCmdBlockTest.forEach(HandlerImpl::unblock);
      replicableCmdBlockTest.clear();
   }

   private static final class HandlerImpl<T> implements BlockHandler, Predicate<T> {

      private final Predicate<T> predicate;
      private final String timeoutMessage;
      private final CountDownLatch commandBlockedLatch = new CountDownLatch(1);
      private final CompletableFuture<Void> afterBlocked = new CompletableFuture<>();

      private HandlerImpl(Predicate<T> predicate, String timeoutMessage) {
         this.predicate = predicate;
         this.timeoutMessage = timeoutMessage;
      }

      @Override
      public boolean isBlocked() {
         return commandBlockedLatch.getCount() > 0;
      }

      @Override
      public void awaitUntilBlocked(Duration timeout) throws InterruptedException {
         assertTrue("Timeout waiting for the command to block: " + timeoutMessage,
               commandBlockedLatch.await(timeout.toNanos(), TimeUnit.NANOSECONDS));
      }

      @Override
      public void unblock() {
         afterBlocked.complete(null);
      }

      @Override
      public boolean test(T t) {
         return predicate.test(t);
      }

      void runAfterBlocked(Runnable action) {
         commandBlockedLatch.countDown();
         afterBlocked.thenRunAsync(action, ForkJoinPool.commonPool());
      }
   }
}
