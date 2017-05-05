package org.infinispan.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A listener that listens for replication events on a cache it is watching.  Typical usage: <code> ReplListener r =
 * attachReplicationListener(cache); r.expect(RemoveCommand.class); // ... r.waitForRPC(); </code>
 */
public class ReplListener {
   Cache<?, ?> c;
   volatile List<Predicate<VisitableCommand>> expectedCommands;
   List<VisitableCommand> eagerCommands = new LinkedList<>();
   boolean recordCommandsEagerly;
   boolean watchLocal;
   final Lock expectationSetupLock = new ReentrantLock();
   CountDownLatch latch = new CountDownLatch(1);
   volatile boolean sawAtLeastOneInvocation = false;
   boolean expectAny = false;
   private Log log = LogFactory.getLog(ReplListener.class);

   /**
    * This listener attaches itself to a cache and when {@link #expect(Class[])} is invoked, will start checking for
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
      this(c, recordCommandsEagerly, false);
   }

   /**
    * Same as {@link #ReplListener(org.infinispan.Cache, boolean)} except that this constructor allows you to set the
    * watchLocal parameter.  If true, even local events are recorded (not just ones that originate remotely).
    *
    * @param c                     cache on which to attach listener
    * @param recordCommandsEagerly whether to record commands eagerly
    * @param watchLocal            if true, local events are watched for as well
    */
   public ReplListener(Cache<?, ?> c, boolean recordCommandsEagerly, boolean watchLocal) {
      this.c = c;
      this.recordCommandsEagerly = recordCommandsEagerly;
      this.watchLocal = watchLocal;
      this.c.getAdvancedCache().addInterceptor(new ReplListenerInterceptor(), 1);
   }

   /**
    * Expects any commands.  The moment a single command is detected, the {@link #waitForRpc()} command will be
    * unblocked.
    */
   public void expectAny() {
      expectAny = true;
      expect(new Predicate[0]);
   }

   /**
    * Expects a specific set of commands, within transactional scope (i.e., as a payload to a PrepareCommand).  If the
    * cache mode is synchronous, a CommitCommand is expected as well.
    *
    * @param commands commands to expect (not counting transaction boundary commands like PrepareCommand and
    *                 CommitCommand)
    */
   @SuppressWarnings("unchecked")
   public void expectWithTx(Class<? extends VisitableCommand>... commands) {
      List<Class<? extends VisitableCommand>> cmdsToExpect = new ArrayList<Class<? extends VisitableCommand>>();
      cmdsToExpect.add(PrepareCommand.class);
      if (commands != null) cmdsToExpect.addAll(Arrays.asList(commands));
      //this is because for async replication we have an 1pc transaction
      if (c.getCacheConfiguration().clustering().cacheMode().isSynchronous()) cmdsToExpect.add(CommitCommand.class);

      expect(cmdsToExpect.toArray(new Class[cmdsToExpect.size()]));
   }

   /**
    * Expects any commands, within transactional scope (i.e., as a payload to a PrepareCommand).  If the cache mode is
    * synchronous, a CommitCommand is expected as well.
    */
   @SuppressWarnings("unchecked")
   public void expectAnyWithTx() {
      List<Class<? extends VisitableCommand>> cmdsToExpect = new ArrayList<Class<? extends VisitableCommand>>(2);
      cmdsToExpect.add(PrepareCommand.class);
      //this is because for async replication we have an 1pc transaction
      if (c.getCacheConfiguration().clustering().cacheMode().isSynchronous()) cmdsToExpect.add(CommitCommand.class);

      expect(cmdsToExpect.toArray(new Class[cmdsToExpect.size()]));
   }

   /**
    * Expects a specific set of commands.  {@link #waitForRpc()} will block until all of these commands are detected.
    *
    * @param expectedCommands commands to expect
    */
   public void expect(Class<? extends VisitableCommand>... expectedCommands) {
      Function<Class<? extends VisitableCommand>, Predicate<VisitableCommand>> predicateGenerator = clazz -> clazz::isInstance;
      expect(Stream.of(expectedCommands).map(predicateGenerator).collect(Collectors.toList()));
   }

   public void expect(Class<? extends VisitableCommand> expectedCommand) {
      expect(Collections.singleton(expectedCommand::isInstance));
   }

   public void expect(Predicate<VisitableCommand> predicate) {
      expect(Collections.singleton(predicate));
   }

   public void expect(Predicate<VisitableCommand>... predicates) {
      expect(Arrays.asList(predicates));
   }

   public void expect(Collection<Predicate<VisitableCommand>> predicates) {
      expectationSetupLock.lock();
      try {
         if (this.expectedCommands == null) {
            this.expectedCommands = new CopyOnWriteArrayList<>();
         }
         this.expectedCommands.addAll(predicates);
         info("Record eagerly is " + recordCommandsEagerly + ", and eager commands are " + eagerCommands);
         if (recordCommandsEagerly) {
            for (VisitableCommand eager : eagerCommands) {
               this.expectedCommands.removeIf(pred -> pred.test(eager));
            }
            if (!eagerCommands.isEmpty()) sawAtLeastOneInvocation = true;
            eagerCommands.clear();
         }
      } finally {
         expectationSetupLock.unlock();
      }
   }

   private void info(String str) {
      log.info(" [" + c + "] " + str);
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
         boolean successful = (expectAny && sawAtLeastOneInvocation) || (!expectAny && expectedCommands.isEmpty());
         info("Expect Any is " + expectAny + ", saw at least one? " + sawAtLeastOneInvocation + " Successful? " + successful);
         if (!successful && !latch.await(time, unit)) {
            EmbeddedCacheManager cacheManager = c.getCacheManager();
            assert false : "Waiting for more than " + time + " " + unit + " and some commands did not replicate on cache [" + cacheManager.getAddress() + "]";
         }
      }
      catch (InterruptedException e) {
         throw new IllegalStateException("unexpected", e);
      }
      finally {
         expectationSetupLock.lock();
         expectedCommands = null;
         expectationSetupLock.unlock();
         expectAny = false;
         sawAtLeastOneInvocation = false;
         latch = new CountDownLatch(1);
         eagerCommands.clear();
      }
   }

   public Cache<?, ?> getCache() {
      return c;
   }

   public void resetEager() {
      eagerCommands.clear();
   }

   public void reconfigureListener(boolean recordCommandsEagerly, boolean watchLocal) {
      this.recordCommandsEagerly = recordCommandsEagerly;
      this.watchLocal = watchLocal;
   }

   private boolean isPrimaryOwner(VisitableCommand cmd) {
      if (cmd instanceof DataCommand) {
         return c.getAdvancedCache().getDistributionManager().getCacheTopology()
               .getDistribution(((DataCommand) cmd).getKey()).isPrimary();
      } else {
         return true;
      }
   }

   protected class ReplListenerInterceptor extends CommandInterceptor {
      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         // first pass up chain
         Object o;
         try {
            o = invokeNextInterceptor(ctx, cmd);
         } finally {//make sure we do mark this command as received even in the case of exceptions(e.g. timeouts)
            info("Checking whether command " + cmd.getClass().getSimpleName() + " should be marked as local with watch local set to " + watchLocal);
            if (!ctx.isOriginLocal() || (watchLocal && isPrimaryOwner(cmd))) markAsVisited(cmd);
         }
         return o;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand cmd) throws Throwable {
         // first pass up chain
         Object o = invokeNextInterceptor(ctx, cmd);
         if (!ctx.isOriginLocal() || watchLocal) {
            markAsVisited(cmd);
            for (WriteCommand mod : cmd.getModifications()) markAsVisited(mod);
         }
         return o;
      }

      private void markAsVisited(VisitableCommand cmd) {
         expectationSetupLock.lock();
         try {
            info("ReplListener saw command " + cmd);
            if (expectedCommands != null) {
               if (expectedCommands.removeIf(predicate -> predicate.test(cmd))) {
                  info("Successfully removed command: " + cmd.getClass());
               }
               else {
                  if (recordCommandsEagerly) eagerCommands.add(cmd);
               }
               sawAtLeastOneInvocation = true;
               if (expectedCommands.isEmpty()) {
                  info("Nothing to wait for, releasing latch");
                  latch.countDown();
               }
            } else {
               if (recordCommandsEagerly) eagerCommands.add(cmd);
            }
         } finally {
            expectationSetupLock.unlock();
         }
      }
   }
}
