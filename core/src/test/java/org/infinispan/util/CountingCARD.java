package org.infinispan.util;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.remoting.transport.jgroups.SingleResponseFuture;
import org.infinispan.test.TestingUtil;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;

/**
 * Dispatcher that counts actually ongoing unicast RPCs. Its purpose is to isolate RPCs started before
 * {@link #advanceGenerationAndAwait(long, TimeUnit)} and those afterwards. It can handle staggered calls as well.
 */
public class CountingCARD extends CommandAwareRpcDispatcher {

   private final GenerationalScheduledThreadPoolExecutor timeoutExecutor;
   private int awaitingReponses;

   public static CountingCARD replaceDispatcher(EmbeddedCacheManager cacheManager) {
      GlobalComponentRegistry gcr = cacheManager.getGlobalComponentRegistry();
      JGroupsTransport transport = (JGroupsTransport) gcr.getComponent(Transport.class);
      InboundInvocationHandler handler = gcr.getComponent(InboundInvocationHandler.class);
      GenerationalScheduledThreadPoolExecutor timeoutExecutor = new GenerationalScheduledThreadPoolExecutor(transport.getAddress().toString());
      TimeService timeService = gcr.getComponent(TimeService.class);
      Executor remoteExecutor = gcr.getComponent(KnownComponentNames.REMOTE_COMMAND_EXECUTOR);
      StreamingMarshaller marshaller = gcr.getComponent(StreamingMarshaller.class);
      CountingCARD instance = new CountingCARD(transport, handler, timeoutExecutor, timeService, remoteExecutor, marshaller);
      TestingUtil.replaceField(instance, "dispatcher", transport, JGroupsTransport.class);
      return instance;
   }

   public CountingCARD(JGroupsTransport transport, InboundInvocationHandler handler,
                       GenerationalScheduledThreadPoolExecutor timeoutExecutor, TimeService timeService,
                       Executor remoteExecutor, StreamingMarshaller marshaller) {
      super(transport.getChannel(), transport, handler, timeoutExecutor, timeService, remoteExecutor, marshaller);
      installUpHandler(prot_adapter, true); // Make sure that this is the up handler
      this.timeoutExecutor = timeoutExecutor;
      start();
   }

   @Override
   protected SingleResponseFuture processSingleCall(ReplicableCommand command, long timeout, Address destination,
                                                    ResponseMode mode, DeliverOrder deliverOrder) throws Exception {
      synchronized (this) {
         awaitingReponses++;
      }
      SingleResponseFuture srf = super.processSingleCall(command, timeout, destination, mode, deliverOrder);
      if (srf == null) {
         synchronized (this) {
            if (--awaitingReponses == 0) notifyAll();
         }
      } else {
         srf.whenComplete((responseRsp, throwable) -> {
            synchronized (CountingCARD.this) {
               if (--awaitingReponses == 0) notifyAll();
            }
         });
      }
      return srf;
   }

   /**
    * Prohibit staggered calls started before call to this happen to execute, and wait until we get responses
    * for all already invoked RPCs.
    */
   public void advanceGenerationAndAwait(long timeout, TimeUnit timeUnit) throws InterruptedException {
      timeoutExecutor.advanceGeneration();
      long now = System.currentTimeMillis();
      long deadline = now + timeUnit.toMillis(timeout);
      synchronized (this) {
         while (awaitingReponses > 0) {
            this.wait(deadline - now);
            now = System.currentTimeMillis();
         }
      }
   }

   /**
    * Executor that records a 'generation' when the task is scheduled, and later, when the task is about
    * to be executed, checks this generation. If the generation has advanced through {@link #advanceGeneration()}
    * the task is silently ignored.
    */
   static class GenerationalScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
      private AtomicLong generation = new AtomicLong();

      public GenerationalScheduledThreadPoolExecutor(String name) {
         super(1, r -> new Thread(r, "counting-timeout-thread-" + name));
         setRemoveOnCancelPolicy(true);
      }

      @Override
      public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
         long taskGeneration = generation.get();
         return super.schedule(() -> {
            if (taskGeneration < generation.get()) {
               return;
            }
            command.run();
         }, delay, unit);
      }

      public void advanceGeneration() {
         generation.incrementAndGet();
      }
   }
}
