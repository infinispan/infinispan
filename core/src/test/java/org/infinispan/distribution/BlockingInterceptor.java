package org.infinispan.distribution;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.util.CompletableFutures;

/**
 * Interceptor that allows for waiting for a command to be invoked, blocking that command and subsequently
 * allowing that command to be released.
 *
 * @author William Burns
 * @since 6.0
 */
public class BlockingInterceptor<T extends VisitableCommand> extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(BlockingInterceptor.class);

   private final CyclicBarrier barrier;
   private final boolean blockAfter;
   private final boolean originLocalOnly;
   private final AtomicBoolean suspended = new AtomicBoolean();
   private final Predicate<VisitableCommand> acceptCommand;

   public BlockingInterceptor(CyclicBarrier barrier, Class<T> commandClass,
         boolean blockAfter, boolean originLocalOnly) {
      this(barrier, commandClass, blockAfter, originLocalOnly, t -> t != null && commandClass.equals(t.getClass()));
   }

   public BlockingInterceptor(CyclicBarrier barrier, Class<T> commandClass,
         boolean blockAfter, boolean originLocalOnly, Predicate<T> acceptCommand) {
      this(barrier, blockAfter, originLocalOnly,
            t -> t != null && commandClass.equals(t.getClass()) && acceptCommand.test(commandClass.cast(t)));
   }

   public BlockingInterceptor(CyclicBarrier barrier, boolean blockAfter, boolean originLocalOnly,
                              Predicate<VisitableCommand> acceptCommand) {
      this.barrier = barrier;
      this.blockAfter = blockAfter;
      this.originLocalOnly = originLocalOnly;
      this.acceptCommand = acceptCommand;
   }

   public void suspend(boolean s) {
      this.suspended.set(s);
   }

   public void proceed() throws Exception {
      barrier.await(30, TimeUnit.SECONDS);
   }

   private CompletionStage<Void> blockIfNeeded(InvocationContext ctx, VisitableCommand command) {
      if (suspended.get()) {
         log.tracef("Suspended, not blocking command %s", command);
         return null;
      }
      if ((!originLocalOnly || ctx.isOriginLocal()) && acceptCommand.test(command)) {
         return CompletableFuture.runAsync(() -> block(command));
      } else {
         log.tracef("Not blocking command %s", command);
         return null;
      }
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) {
      return blockAfter ?
            invokeNextAndHandle(ctx, command, this::blockAfterIfNeeded) :
            asyncInvokeNext(ctx, command, blockIfNeeded(ctx, command));
   }

   private void block(VisitableCommand cmd) {
      try {
         log.tracef("Command blocking %s completion of %s", blockAfter ? "after" : "before", cmd);
         // The first arrive and await is to sync with main thread
         barrier.await(30, TimeUnit.SECONDS);
         // Now we actually block until main thread lets us go
         barrier.await(30, TimeUnit.SECONDS);
         log.tracef("Command completed blocking completion of %s", cmd);
      } catch (Exception e) {
         throw CompletableFutures.wrapAsCompletionException(e);
      }
   }

   private Object blockAfterIfNeeded(InvocationContext ctx, VisitableCommand cmd, Object rv, Throwable t) {
      return delayedValue(blockIfNeeded(ctx, cmd), rv, t);
   }
}
