package org.infinispan.partitionhandling.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * @author Dan Berindei
 * @since 7.0
 */
public interface PartitionHandlingManager {
   AvailabilityMode getAvailabilityMode();

   CompletionStage<Void> setAvailabilityMode(AvailabilityMode availabilityMode);

   void checkWrite(Object key);

   void checkRead(Object key, long flagBitSet);

   void checkClear();

   void checkBulkRead();

   @Deprecated //test use only. it can be removed if we update the tests
   CacheTopology getLastStableTopology();

   /**
    * Adds a partially aborted transaction.
        * The transaction should be registered when it is not sure if the abort happens successfully in all the affected
    * nodes.
    *
    * @param globalTransaction the global transaction.
    * @param affectedNodes     the nodes involved in the transaction and they must abort the transaction.
    * @param lockedKeys        the keys locally locked.
    * @return {@code true} if the {@link PartitionHandlingManager} will handle it, {@code false} otherwise.
    */
   boolean addPartialRollbackTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes,
                                         Collection<Object> lockedKeys);

   /**
    * Adds a partially committed transaction.
        * The transaction is committed in the second phase and it is register if it is not sure that the transaction was
    * committed successfully in all the affected nodes.
    *
    * @param globalTransaction the global transaction.
    * @param affectedNodes     the nodes involved in the transaction and they must commit it.
    * @param lockedKeys        the keys locally locked.
    * @param newVersions       the updated versions. Only used when versioning is enabled.
    * @return {@code true} if the {@link PartitionHandlingManager} will handle it, {@code false} otherwise.
    */
   boolean addPartialCommit2PCTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes,
                                          Collection<Object> lockedKeys, Map<Object, IncrementableEntryVersion> newVersions);

   /**
    * Adds a partially committed transaction.
        * The transaction is committed in one phase and it is register if it is not sure that the transaction was committed
    * successfully in all the affected nodes.
    *
    * @param globalTransaction the global transaction.
    * @param affectedNodes     the nodes involved in the transaction and they must commit it.
    * @param lockedKeys        the keys locally locked.
    * @param modifications     the transaction's modification log.
    * @return {@code true} if the {@link PartitionHandlingManager} will handle it, {@code false} otherwise.
    */
   boolean addPartialCommit1PCTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes,
                                          Collection<Object> lockedKeys, List<WriteCommand> modifications);

   /**
    * It checks if the transaction resources (for example locks) can be released.
        * The transaction resource can't be released when the transaction is partially committed.
    *
    * @param globalTransaction the transaction.
    * @return {@code true} if the resources can be released, {@code false} otherwise.
    */
   boolean isTransactionPartiallyCommitted(GlobalTransaction globalTransaction);

   /**
    * @return a collection of partial committed or aborted transactions.
    */
   Collection<GlobalTransaction> getPartialTransactions();

   /**
    * It checks if the transaction can be aborted when the originator leaves the cluster.
        * The only case in which it is not possible to abort is when partition handling is enabled and the originator didn't
    * leave gracefully. The transaction will complete when the partition heals.
    *
    * @param globalTransaction the global transaction.
    * @return {@code true} if the transaction can be aborted, {@code false} otherwise.
    */
   boolean canRollbackTransactionAfterOriginatorLeave(GlobalTransaction globalTransaction);

   /**
    * Notifies the {@link PartitionHandlingManager} that the cache topology was update.
        * It detects when the partition is merged and tries to complete all the partially completed transactions.
    *
    * @param cacheTopology the new cache topology.
    */
   void onTopologyUpdate(CacheTopology cacheTopology);

}
