package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A listener that listens for replication events on a cache it is watching.  Typical usage: <code> ReplListener r =
 * attachReplicationListener(cache); r.expect(RemoveCommand.class); // ... r.waitForRPC(); </code>
 */
public class ReplListener {
   Cache<?, ?> c;
   volatile Set<Class<? extends VisitableCommand>> expectedCommands;
   Set<Class<? extends VisitableCommand>> eagerCommands;
   boolean recordCommandsEagerly;
   CountDownLatch latch = new CountDownLatch(1);

   /**
    * This listener atatches itself to a cache and when {@link #expect(Class[])} is invoked, will start checking for
    * invocations of the command on the cache, waiting for all expected commands to be received in {@link
    * #waitForRpc()}.
    *
    * @param c cache on which to attach listener
    */
   public ReplListener(Cache<?, ?> c) {
      this(c, false);
   }

   /**
    * As {@link #ReplListener(org.infinispan.Cache)} except that you can optionally configure whether command recording
    * is eager (false by default).
    * <p/>
    * If <tt>recordCommandsEagerly</tt> is true, then commands are recorded from the moment the listener is attached to
    * the cache, even before {@link #expect(Class[])} is invoked.  As such, when {@link #expect(Class[])} is called, the
    * list of commands to wait for will take into account commands already seen thanks to eager recording.
    *
    * @param c                     cache on which to attach listener
    * @param recordCommandsEagerly whether to record commands eagerly
    */
   public ReplListener(Cache<?, ?> c, boolean recordCommandsEagerly) {
      this.c = c;
      this.recordCommandsEagerly = recordCommandsEagerly;
      this.c.getAdvancedCache().addInterceptor(new ReplListenerInterceptor(), 1);
   }

   /**
    * Expects any commands.  The moment a single command is detected, the {@link #waitForRpc()} command will be
    * unblocked.
    */
   public void expectAny() {
      expect();
   }

   /**
    * Expects a specific set of commands, within transactional scope (i.e., as a payload to a PrepareCommand).  If the
    * cache mode is synchronous, a CommitCommand is expected as well.
    *
    * @param commands commands to expect (not counting transaction boundary commands like PrepareCommand and
    *                 CommitCommand)
    */
   public void expectWithTx(Class<? extends VisitableCommand>... commands) {
      expect(PrepareCommand.class);
      expect(commands);
      //this is because for async replication we have an 1pc transaction
      if (c.getConfiguration().getCacheMode().isSynchronous()) expect(CommitCommand.class);
   }

   /**
    * Expects any commands, within transactional scope (i.e., as a payload to a PrepareCommand).  If the cache mode is
    * synchronous, a CommitCommand is expected as well.
    */
   public void expectAnyWithTx() {
      expect(PrepareCommand.class);
      //this is because for async replication we have an 1pc transaction
      if (c.getConfiguration().getCacheMode().isSynchronous()) expect(CommitCommand.class);
   }

   /**
    * Expects a specific set of commands.  {@link #waitForRpc()} will block until all of these commands are detected.
    *
    * @param expectedCommands commands to expect
    */
   public void expect(Class<? extends VisitableCommand>... expectedCommands) {
      if (this.expectedCommands == null) {
         this.expectedCommands = new HashSet<Class<? extends VisitableCommand>>();
      }
      this.expectedCommands.addAll(Arrays.asList(expectedCommands));

      if (recordCommandsEagerly) this.expectedCommands.removeAll(eagerCommands);
   }

   /**
    * Blocks for a predefined amount of time (120 Seconds) until commands defined in any of the expect*() methods have
    * been detected.  If the commands have not been detected by this time, an exception is thrown.
    */
   public void waitForRpc() {
      waitForRpc(30, TimeUnit.SECONDS);
   }

   /**
    * The same as {@link #waitForRpc()} except that you are allowed to specify the max wait time.
    */
   public void waitForRpc(long time, TimeUnit unit) {
      assert expectedCommands != null : "there are no replication expectations; please use ReplListener.expect() before calling this method";
      try {
         if (!latch.await(time, unit)) {
            assert false : "Waiting for more than " + time + " " + unit + " and following commands did not replicate: " + expectedCommands;
         }
      }
      catch (InterruptedException e) {
         throw new IllegalStateException("unexpected", e);
      }
      finally {
         expectedCommands = null;
         latch = new CountDownLatch(1);
      }
   }

   public Cache<?, ?> getCache() {
      return c;
   }

   protected class ReplListenerInterceptor extends CommandInterceptor {
      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         // first pass up chain
         Object o = invokeNextInterceptor(ctx, cmd);
         if (!ctx.isOriginLocal()) markAsVisited(cmd);
         return o;
      }

      @Override
      public Object visitPrepareCommand(InvocationContext ctx, PrepareCommand cmd) throws Throwable {
         // first pass up chain
         Object o = invokeNextInterceptor(ctx, cmd);
         if (!ctx.isOriginLocal()) {
            markAsVisited(cmd);
            for (WriteCommand mod : cmd.getModifications()) markAsVisited(mod);
         }
         return o;
      }

      private void markAsVisited(VisitableCommand cmd) {
         if (expectedCommands != null) {
            expectedCommands.remove(cmd.getClass());
            if (expectedCommands.isEmpty()) latch.countDown();
         } else {
            if (recordCommandsEagerly) {
               eagerCommands.add(cmd.getClass());
            } else {
               System.out.println("Received unexpected command: " + cmd);
            }
         }
      }
   }
}
