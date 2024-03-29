package org.infinispan.distribution;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

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
public class BlockingInterceptor<T extends VisitableCommand> extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(BlockingInterceptor.class);

   private final CyclicBarrier barrier;
   private final boolean blockAfter;
   private final boolean originLocalOnly;
   private final AtomicBoolean suspended = new AtomicBoolean();
   private final Predicate<VisitableCommand> acceptCommand;

   public BlockingInterceptor(CyclicBarrier barrier, Class<T> commandClass,
         boolean blockAfter, boolean originLocalOnly) {
      this(barrier, commandClass, blockAfter, originLocalOnly, t -> t != null && commandClass == t.getClass());
   }

   public BlockingInterceptor(CyclicBarrier barrier, Class<T> commandClass,
         boolean blockAfter, boolean originLocalOnly, Predicate<T> acceptCommand) {
      this(barrier, blockAfter, originLocalOnly,
            t -> t != null && commandClass == t.getClass() && acceptCommand.test(commandClass.cast(t)));
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

   private void blockIfNeeded(InvocationContext ctx, VisitableCommand command) throws Exception {
      if (suspended.get()) {
         log.tracef("Suspended, not blocking command %s", command);
         return;
      }
      if ((!originLocalOnly || ctx.isOriginLocal()) && acceptCommand.test(command)) {
         log.tracef("Command blocking %s completion of %s", blockAfter ? "after" : "before", command);
         // The first arrive and await is to sync with main thread
         barrier.await(30, TimeUnit.SECONDS);
         // Now we actually block until main thread lets us go
         barrier.await(30, TimeUnit.SECONDS);
         log.tracef("Command completed blocking completion of %s", command);
      } else {
         log.tracef("Not blocking command %s", command);
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
