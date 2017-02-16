package org.infinispan.distribution;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor that allows for waiting for a command to be invoked, blocking that command and subsequently
 * allowing that command to be released.
 *
 * @author William Burns
 * @since 6.0
 */
public class BlockingInterceptor extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(BlockingInterceptor.class);

   private final CyclicBarrier barrier;
   private final Class<? extends VisitableCommand> commandClass;
   private final boolean blockAfter;
   private final boolean originLocalOnly;
   private final AtomicBoolean suspended = new AtomicBoolean();

   public BlockingInterceptor(CyclicBarrier barrier, Class<? extends VisitableCommand> commandClass,
         boolean blockAfter, boolean originLocalOnly) {
      this.barrier = barrier;
      this.commandClass = commandClass;
      this.blockAfter = blockAfter;
      this.originLocalOnly = originLocalOnly;
   }

   public void suspend(boolean s) {
      this.suspended.set(s);
   }

   private void blockIfNeeded(InvocationContext ctx, VisitableCommand command) throws BrokenBarrierException, InterruptedException {
      if (suspended.get()) {
         log.tracef("Suspended, not blocking command %s", command);
         return;
      }
      if (commandClass.equals(command.getClass()) && (!originLocalOnly || ctx.isOriginLocal())) {
         log.tracef("Command blocking %s completion of %s", blockAfter ? "after" : "before", command);
         // The first arrive and await is to sync with main thread
         barrier.await();
         // Now we actually block until main thread lets us go
         barrier.await();
         log.tracef("Command completed blocking completion of %s", command);
      } else {
         log.trace("Command arrived but already found a blocker");
      }
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (!blockAfter) {
         blockIfNeeded(ctx, command);
      }
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         if (blockAfter) {
            blockIfNeeded(rCtx, rCommand);
         }
      });
   }
}
