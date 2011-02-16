package org.infinispan.distribution;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;

/**
 * This abstraction performs RPCs and works on a TransactionLogger located on a remote node.
 *
 * @author Manik Surtani
 * @since 4.2.1
 */
public interface RemoteTransactionLogger {
   /**
    * Drains the transaction log and returns a list of what has been drained.
    *
    * @return a list of drained commands
    */
   List<WriteCommand> drain();

   /**
    * Similar to {@link #drain()} except that relevant locks are acquired so that no more commands are added to the
    * transaction log during this process, and transaction logging is disabled after draining.
    *
    * @return list of drained commands
    */
   List<WriteCommand> drainAndLock(Address lockFor);

   /**
    * Tests whether the drain() method can be called without a lock.  This is usually true if there is a lot of stuff to
    * drain.  After a certain threshold (once there are relatively few entries in the tx log) this will return false
    * after which you should call drainAndLock() to clear the final parts of the log.
    *
    * @return true if drain() should be called, false if drainAndLock() should be called.
    */
   boolean shouldDrainWithoutLock();

   /**
    * Drains pending prepares.  Note that this should *only* be done after calling drainAndLock() to prevent race
    * conditions
    *
    * @return a list of prepares pending commit or rollback
    */
   Collection<PrepareCommand> getPendingPrepares();

   /**
    * Unlocks and disables the transaction logger.  Should <i>only</i> be called after {@link #drainAndLock()}.
    */
   void unlockAndDisable(Address lockedFor);
}
