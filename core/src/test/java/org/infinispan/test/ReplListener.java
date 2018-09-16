package org.infinispan.test;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.jcip.annotations.GuardedBy;
import org.infinispan.Cache;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A listener that listens for replication events on a cache it is watching.  Typical usage: <code> ReplListener r =
 * attachReplicationListener(cache); r.expect(RemoveCommand.class); // ... r.waitForRPC(); </code>
 */
public class ReplListener {
   private static final Log log = LogFactory.getLog(ReplListener.class);

   private final Cache<?, ?> cache;
   private final Lock lock = new ReentrantLock();
   private final Condition newCommandCondition = lock.newCondition();
   @GuardedBy("lock")
   private final List<Predicate<VisitableCommand>> expectedCommands = new ArrayList<>();
   @GuardedBy("lock")
   private final Queue<VisitableCommand> loggedCommands = new ArrayDeque<>();
   @GuardedBy("lock")
   private boolean watchLocal;

   /**
    * This listener attaches itself to a cache and when {@link #expect(Class[])} is invoked, will start checking for
    * invocations of the command on the cache, waiting for all expected commands to be received in {@link
    * #waitForRpc()}.
    *
    * @param cache cache on which to attach listener
    */
   public ReplListener(Cache<?, ?> cache) {
      this(cache, false);
   }

   /**
    * As {@link #ReplListener(org.infinispan.Cache)} except that you can optionally configure whether command recording
    * is eager (false by default).
    * <p>
    * If <tt>recordCommandsEagerly</tt> is true, then commands are recorded from the moment the listener is attached to
    * the cache, even before {@link #expect(Class[])} is invoked.  As such, when {@link #expect(Class[])} is called, the
    * list of commands to wait for will take into account commands already seen thanks to eager recording.
    *
    * @param cache                     cache on which to attach listener
    * @param recordCommandsEagerly whether to record commands eagerly
    */
   public ReplListener(Cache<?, ?> cache, boolean recordCommandsEagerly) {
      this(cache, recordCommandsEagerly, false);
   }

   /**
    * Same as {@link #ReplListener(org.infinispan.Cache, boolean)} except that this constructor allows you to set the
    * watchLocal parameter.  If true, even local events are recorded (not just ones that originate remotely).
    *
    * @param cache                     cache on which to attach listener
    * @param recordCommandsEagerly whether to record commands eagerly
    * @param watchLocal            if true, local events are watched for as well
    */
   public ReplListener(Cache<?, ?> cache, boolean recordCommandsEagerly, boolean watchLocal) {
      this.cache = cache;
      this.watchLocal = watchLocal;
      this.cache.getAdvancedCache().addInterceptor(new ReplListenerInterceptor(), 1);
   }

   /**
    * Expects any commands.  The moment a single command is detected, the {@link #waitForRpc()} command will be
    * unblocked.
    */
   public void expectAny() {
      expect(c -> true);
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
      List<Class<? extends VisitableCommand>> cmdsToExpect = new ArrayList<>();
      cmdsToExpect.add(PrepareCommand.class);
      if (commands != null) cmdsToExpect.addAll(Arrays.asList(commands));
      //this is because for async replication we have an 1pc transaction
      if (cache.getCacheConfiguration().clustering().cacheMode().isSynchronous()) cmdsToExpect.add(CommitCommand.class);

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
      if (cache.getCacheConfiguration().clustering().cacheMode().isSynchronous()) cmdsToExpect.add(CommitCommand.class);

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
      lock.lock();
      try {
         this.expectedCommands.addAll(predicates);
      } finally {
         lock.unlock();
      }
   }

   private void debugf(String format, Object... params) {
      log.debugf("[" + cache.getCacheManager().getAddress() + "] " + format, params);
   }

   /**
    * Blocks for a predefined amount of time (30 Seconds) until commands defined in any of the expect*() methods have
    * been detected.  If the commands have not been detected by this time, an exception is thrown.
    */
   public void waitForRpc() {
      waitForRpc(30, TimeUnit.SECONDS);
   }

   /**
    * The same as {@link #waitForRpc()} except that you are allowed to specify the max wait time.
    */
   public void waitForRpc(long time, TimeUnit unit) {
      assertFalse("there are no replication expectations; please use ReplListener.expect() before calling this method",
                  expectedCommands.isEmpty());
      lock.lock();
      try {
         long remainingNanos = unit.toNanos(time);
         while (true) {
            debugf("Waiting for %d command(s)", expectedCommands.size());
            for (Iterator<VisitableCommand> itCommand = loggedCommands.iterator(); itCommand.hasNext(); ) {
               VisitableCommand command = itCommand.next();
               for (Iterator<Predicate<VisitableCommand>> itExpectation = expectedCommands.iterator();
                    itExpectation.hasNext(); ) {
                  Predicate<VisitableCommand> expectation = itExpectation.next();
                  if (expectation.test(command)) {
                     debugf("Matched command %s", command);
                     itCommand.remove();
                     itExpectation.remove();
                     break;
                  }
               }
            }
            if (expectedCommands.isEmpty()) {
               newCommandCondition.signalAll();
            }
            if (expectedCommands.isEmpty())
               break;

            remainingNanos = newCommandCondition.awaitNanos(remainingNanos);
            Address address = cache.getCacheManager().getAddress();
            assertTrue("Waiting for more than " + time + " " + unit +
                          " and some commands did not replicate on cache [" + address + "]",
                       remainingNanos > 0);
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new TestException("Interrupted", e);
      } finally {
         lock.unlock();
      }
   }

   public Cache<?, ?> getCache() {
      return cache;
   }

   public void resetEager() {
      lock.lock();
      try {
         loggedCommands.clear();
      } finally {
         lock.unlock();
      }

   }

   public void reconfigureListener(boolean watchLocal) {
      lock.lock();
      try {
         this.watchLocal = watchLocal;
      } finally {
         lock.unlock();
      }

   }

   private boolean isPrimaryOwner(VisitableCommand cmd) {
      if (cmd instanceof DataCommand) {
         return cache.getAdvancedCache().getDistributionManager().getCacheTopology()
                     .getDistribution(((DataCommand) cmd).getKey()).isPrimary();
      } else {
         return true;
      }
   }

   protected class ReplListenerInterceptor extends CommandInterceptor {
      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         try {
            if (!ctx.isOriginLocal() || (watchLocal && isPrimaryOwner(cmd))) {
               debugf("Delaying command %s", cmd);
               TestingUtil.sleepRandom(10);
            }
            // pass up chain
            return invokeNextInterceptor(ctx, cmd);
         } finally {//make sure we do mark this command as received even in the case of exceptions(e.g. timeouts)
            if (!ctx.isOriginLocal() || (watchLocal && isPrimaryOwner(cmd))) {
               logCommand(cmd);
            } else {
               debugf("Not logging command (watchLocal=%b) %s", watchLocal, cmd);
            }
         }
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand cmd) throws Throwable {
         try {
            // first pass up chain
            return invokeNextInterceptor(ctx, cmd);
         } finally {
            if (!ctx.isOriginLocal() || watchLocal) {
               logCommand(cmd);
               for (WriteCommand mod : cmd.getModifications()) logCommand(mod);
            }
         }
      }

      private void logCommand(VisitableCommand cmd) {
         lock.lock();
         try {
            debugf("ReplListener saw command %s", cmd);
            loggedCommands.add(cmd);
            newCommandCondition.signalAll();
         } finally {
            lock.unlock();
         }
      }
   }
}
