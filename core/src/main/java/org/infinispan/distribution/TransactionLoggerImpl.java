package org.infinispan.distribution;

import org.infinispan.commands.write.WriteCommand;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
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
      loggingLock.readLock().lock();
      try {
         if (enabled) {
            commandQueue.add(command);
            return true;
         } else {
            return false;
         }
      } finally {
         loggingLock.readLock().unlock();
      }
   }

   public boolean logIfNeeded(Collection<WriteCommand> commands) {
      loggingLock.readLock().lock();
      try {
         if (enabled) {
            for (WriteCommand command : commands) commandQueue.add(command);
            return true;
         } else {
            return false;
         }
      } finally {
         loggingLock.readLock().unlock();
      }
   }

   public int size() {
      return enabled ? 0 : commandQueue.size();
   }

   public boolean isEnabled() {
      return enabled;
   }
}
