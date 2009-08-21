package org.infinispan.distribution;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransactionLoggerImpl implements TransactionLogger {
   volatile boolean enabled;
   final ReadWriteLock loggingLock = new ReentrantReadWriteLock();
   final BlockingQueue<WriteCommand> commandQueue = new LinkedBlockingQueue<WriteCommand>();
   final Map<GlobalTransaction, PrepareCommand> uncommittedPrepares = new ConcurrentHashMap<GlobalTransaction, PrepareCommand>();

   public void enable() {
      enabled = true;
   }

   public List<WriteCommand> drain() {
      List<WriteCommand> list = new LinkedList<WriteCommand>();
      commandQueue.drainTo(list);
      return list;
   }

   public List<WriteCommand> drainAndLock() {
      loggingLock.writeLock().lock();
      return drain();
   }

   public void unlockAndDisable() {
      enabled = false;
      loggingLock.writeLock().unlock();
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
      if (enabled) {
         loggingLock.readLock().lock();
         try {
            if (enabled) {
               uncommittedPrepares.put(command.getGlobalTransaction(), command);
            }
         } finally {
            loggingLock.readLock().unlock();
         }
      }
   }

   public void logIfNeeded(CommitCommand command) {
      if (enabled) {
         loggingLock.readLock().lock();
         try {
            if (enabled) {
               PrepareCommand pc = uncommittedPrepares.remove(command.getGlobalTransaction());
               // TODO how can we handle this efficiently and safely?
//               for (WriteCommand wc : pc.getModifications())
//                  try {
//                     commandQueue.put(wc);
//                  } catch (InterruptedException e) {
//                     Thread.currentThread().interrupt();
//                  }
            }
         } finally {
            loggingLock.readLock().unlock();
         }
      }
   }

   public void logIfNeeded(RollbackCommand command) {
      if (enabled) {
         loggingLock.readLock().lock();
         try {
            if (enabled) {
               uncommittedPrepares.remove(command.getGlobalTransaction());
            }
         } finally {
            loggingLock.readLock().unlock();
         }
      }
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

   public int size() {
      return enabled ? 0 : commandQueue.size();
   }

   public boolean isEnabled() {
      return enabled;
   }
}
