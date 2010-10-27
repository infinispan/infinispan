package org.infinispan.distribution;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A transaction logger to log ongoing transactions in an efficient and thread-safe manner while a rehash is going on.
 * <p/>
 * Transaction logs can then be replayed after the state transferred during a rehash has been written.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransactionLoggerImpl implements TransactionLogger {
   volatile boolean enabled;
   final ReadWriteLock loggingLock = new ReentrantReadWriteLock();
   final BlockingQueue<WriteCommand> commandQueue = new LinkedBlockingQueue<WriteCommand>();
   final Map<GlobalTransaction, PrepareCommand> uncommittedPrepares = new ConcurrentHashMap<GlobalTransaction, PrepareCommand>();
   private static final Log log = LogFactory.getLog(TransactionLoggerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   // the number of transactions after which we need to lock and drain
   private static final int DRAIN_LOCK_THRESHOLD = 10;

   public void enable() {
      log.info("Starting transaction logging");
      enabled = true;
   }

   public List<WriteCommand> drain() {
      List<WriteCommand> list = new LinkedList<WriteCommand>();
      commandQueue.drainTo(list);
      if (trace) log.trace("Drained transaction log to {0}", list);
      return list;
   }

   public List<WriteCommand> drainAndLock() {
      loggingLock.writeLock().lock();
      return drain();
   }

   public void unlockAndDisable() {
      enabled = false;
      Util.safeRelease(loggingLock.writeLock());
      log.info("Stopping transaction logging");
   }

   public boolean logIfNeeded(WriteCommand command) {
      if (enabled) {
         loggingLock.readLock().lock();
         try {
            if (enabled) {
               try {
                  commandQueue.put(command);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
               return true;
            }
         } finally {
            loggingLock.readLock().unlock();
         }
      }
      return false;
   }

   public void logIfNeeded(PrepareCommand command) {
      if (command.isOnePhaseCommit()) {
         if (enabled) {
            loggingLock.readLock().lock();
            try {
               if (enabled) {
                  if (trace) log.trace("Logging 1PC prepare for tx {0}", command.getGlobalTransaction());
                  logModificationsInTransaction(command);
               }
            } finally {
               loggingLock.readLock().unlock();
            }
         }
      } else {
         if (trace) log.trace("Logging 2PC prepare for tx {0}", command.getGlobalTransaction());
         uncommittedPrepares.put(command.getGlobalTransaction(), command);
      }
   }

   private void logModificationsInTransaction(PrepareCommand command) {
      for (WriteCommand wc : command.getModifications()) {
         try {
            commandQueue.put(wc);
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
      }
   }

   public void logIfNeeded(CommitCommand command) {
      PrepareCommand pc = uncommittedPrepares.remove(command.getGlobalTransaction());
      if (enabled) {
         loggingLock.readLock().lock();
         try {
            if (enabled) {
               if (trace) log.trace("Logging commit for tx {0}", command.getGlobalTransaction());
               logModificationsInTransaction(pc);
            }
         } finally {
            loggingLock.readLock().unlock();
         }
      }
   }

   public void logIfNeeded(RollbackCommand command) {
      if (trace) log.trace("Logging rollback for tx {0}", command.getGlobalTransaction());
      uncommittedPrepares.remove(command.getGlobalTransaction());
   }

   public boolean logIfNeeded(Collection<WriteCommand> commands) {
      if (enabled) {
         loggingLock.readLock().lock();
         try {
            if (enabled) {
               for (WriteCommand command : commands)
                  try {
                     commandQueue.put(command);
                  } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                  }
               return true;
            }
         } finally {
            loggingLock.readLock().unlock();
         }
      }
      return false;
   }

   private int size() {
      return enabled ? 0 : commandQueue.size();
   }

   public boolean isEnabled() {
      return enabled;
   }

   public boolean shouldDrainWithoutLock() {
      return size() > DRAIN_LOCK_THRESHOLD;
   }

   public Collection<PrepareCommand> getPendingPrepares() {
      Collection<PrepareCommand> commands = new HashSet<PrepareCommand>(uncommittedPrepares.values());
      uncommittedPrepares.clear();
      return commands;
   }

   @Override
   public String toString() {
      return "TransactionLoggerImpl{" +
            "commandQueue=" + commandQueue +
            ", uncommittedPrepares=" + uncommittedPrepares +
            '}';
   }
}
