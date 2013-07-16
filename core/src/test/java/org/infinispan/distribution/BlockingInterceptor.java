package org.infinispan.distribution;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Interceptor that allows for waiting for a command to be invoked, blocking that command and subsequently
 * allowing that command to be released.
 *
 * @author William Burns
 * @since 6.0
 */
public class BlockingInterceptor extends CommandInterceptor {
   private final CyclicBarrier barrier;
   private final AtomicBoolean firstBlocked = new AtomicBoolean();
   private final Class<? extends VisitableCommand> commandClass;
   private final boolean blockAfter;

   public BlockingInterceptor(CyclicBarrier barrier, Class<? extends VisitableCommand> commandClass,
                              boolean blockAfter) {
      this.barrier = barrier;
      this.commandClass = commandClass;
      this.blockAfter = blockAfter;
   }

   private void blockIfNeeded(VisitableCommand command) throws BrokenBarrierException, InterruptedException {
      if (commandClass.isInstance(command)) {
         if (firstBlocked.compareAndSet(false, true)) {
            try {
               getLog().tracef("Command blocking %s completion of %s", blockAfter ? "after" : "before", command);
               // The first arrive and await is to sync with main thread
               barrier.await();
               // Now we actually block until main thread lets us go
               barrier.await();
               getLog().tracef("Command completed blocking completion of %s", command);
            } finally {
               firstBlocked.set(false);
            }
         } else {
            getLog().trace("Command arrived but already found a blocker");
         }
      }
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      try {
         if (!blockAfter) {
            blockIfNeeded(command);
         }
         return super.handleDefault(ctx, command);
      } finally {
         if (blockAfter) {
            blockIfNeeded(command);
         }
      }
   }
}