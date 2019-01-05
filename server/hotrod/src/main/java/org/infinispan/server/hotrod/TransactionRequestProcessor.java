package org.infinispan.server.hotrod;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import javax.security.auth.Subject;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.PrepareCoordinator;
import org.infinispan.server.hotrod.tx.operation.CommitTransactionOperation;
import org.infinispan.server.hotrod.tx.operation.RollbackTransactionOperation;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

class TransactionRequestProcessor extends CacheRequestProcessor {
   private static final Log log = LogFactory.getLog(TransactionRequestProcessor.class, Log.class);
   private static final boolean isTrace = log.isTraceEnabled();

   TransactionRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor, server);
   }

   private void writeTransactionResponse(HotRodHeader header, int value) {
      writeResponse(header, createTransactionResponse(header, value));
   }

   /**
    * Handles a rollback request from a client.
    */
   void rollbackTransaction(HotRodHeader header, Subject subject, XidImpl xid) {
      RollbackTransactionOperation operation = new RollbackTransactionOperation(header, server, subject, xid,
            this::writeTransactionResponse);
      executor.execute(operation);
   }

   /**
    * Handles a commit request from a client
    */
   void commitTransaction(HotRodHeader header, Subject subject, XidImpl xid) {
      CommitTransactionOperation operation = new CommitTransactionOperation(header, server, subject, xid,
            this::writeTransactionResponse);
      executor.execute(operation);
   }

   /**
    * Handles a prepare request from a client
    */
   void prepareTransaction(HotRodHeader header, Subject subject, XidImpl xid, boolean onePhaseCommit,
         List<TransactionWrite> writes, boolean recoverable, long timeout) {
      HotRodServer.CacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      validateConfiguration(cache);
      executor.execute(() -> prepareTransactionInternal(header, cache, cacheInfo.versionGenerator, xid, onePhaseCommit,
                                                        writes, recoverable, timeout));
   }

   void forgetTransaction(HotRodHeader header, Subject subject, XidImpl xid) {
      //TODO authentication?
      GlobalTxTable txTable = server.getCacheManager().getGlobalComponentRegistry().getComponent(GlobalTxTable.class);
      executor.execute(() -> {
         try {
            txTable.forgetTransaction(xid);
            writeSuccess(header);
         } catch (Throwable t) {
            writeException(header, t);
         }
      });
   }

   void getPreparedTransactions(HotRodHeader header, Subject subject) {
      //TODO authentication?
      if (isTrace) {
         log.trace("Fetching transactions for recovery");
      }
      executor.execute(() -> {
         try {
            GlobalTxTable txTable = server.getCacheManager().getGlobalComponentRegistry()
                  .getComponent(GlobalTxTable.class);
            Collection<Xid> preparedTx = txTable.getPreparedTransactions();
            writeResponse(header, createRecoveryResponse(header, preparedTx));
         } catch (Throwable t) {
            writeException(header, t);
         }
      });
   }

   private void prepareTransactionInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache,
                                           VersionGenerator versionGenerator, XidImpl xid,
                                           boolean onePhaseCommit, List<TransactionWrite> writes,
                                           boolean recoverable, long timeout) {
      try {
         if (writes.isEmpty()) {
            //the client can optimize and avoid contacting the server when no data is written.
            if (isTrace) {
               log.tracef("Transaction %s is read only.", xid);
            }
            writeResponse(header, createTransactionResponse(header, XAResource.XA_RDONLY));
            return;
         }
         PrepareCoordinator prepareCoordinator = new PrepareCoordinator(cache, xid, recoverable, timeout);

         if (checkExistingTxForPrepare(header, prepareCoordinator)) {
            if (isTrace) {
               log.tracef("Transaction %s conflicts with another node.", xid);
            }
            return;
         }

         if (!prepareCoordinator.startTransaction()) {
            if (isTrace) {
               log.tracef("Unable to start transaction %s", xid);
            }
            writeNotExecuted(header);
            return;
         }

         //forces the write-lock. used by pessimistic transaction. it ensures the key is not written after is it read and validated
         //optimistic transaction will use the write-skew check.
         AdvancedCache<byte[], byte[]> txCache = prepareCoordinator.decorateCache(cache);

         try {
            boolean rollback = false;
            for (TransactionWrite write : writes) {
               if (isValid(write, txCache)) {
                  if (write.isRemove()) {
                     txCache.remove(write.key);
                  } else {
                     write.metadata.version(versionGenerator.generateNew());
                     txCache.put(write.key, write.value, write.metadata.build());
                  }
               } else {
                  prepareCoordinator.setRollbackOnly();
                  rollback = true;
                  break;
               }
            }
            int xaCode = rollback ?
                         prepareCoordinator.rollback() :
                         prepareCoordinator.prepare(onePhaseCommit);
            writeResponse(header, createTransactionResponse(header, xaCode));
         } catch (Exception e) {
            writeResponse(header, createTransactionResponse(header, prepareCoordinator.rollback()));
         } finally {
            EmbeddedTransactionManager.dissociateTransaction();
         }
      } catch (Throwable t) {
         log.debugf(t, "Exception while replaying transaction %s for cache %s", xid, cache.getName());
         writeException(header, t);
      }
   }

   /**
    * Checks if the configuration (and the transaction manager) is able to handle client transactions.
    */
   private void validateConfiguration(AdvancedCache<byte[], byte[]> cache) {
      Configuration configuration = cache.getCacheConfiguration();
      if (!configuration.transaction().transactionMode().isTransactional()) {
         throw log.expectedTransactionalCache(cache.getName());
      }
      if (configuration.locking().isolationLevel() != IsolationLevel.REPEATABLE_READ) {
         throw log.unexpectedIsolationLevel(cache.getName());
      }

      //TODO because of ISPN-7672, optimistic and total order transactions needs versions. however, versioning is currently broken
      if (configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC ||
            configuration.transaction().transactionProtocol() == TransactionProtocol.TOTAL_ORDER) {
         //no Log. see comment above
         throw new IllegalStateException(
               String.format("Cache '%s' cannot use Optimistic neither Total Order transactions.", cache.getName()));
      }
   }

   /**
    * Checks if the transaction was already prepared in another node
    * <p>
    * The client can send multiple requests to the server (in case of timeout or similar). This request is ignored when
    * (1) the originator is still alive; (2) the transaction is prepared or committed/rolled-back
    * <p>
    * If the transaction isn't prepared and the originator left the cluster, the previous transaction is rolled-back and
    * a new one is started.
    */
   private boolean checkExistingTxForPrepare(HotRodHeader header, PrepareCoordinator txCoordinator) {
      TxState txState = txCoordinator.getTxState();
      if (txState == null) {
         return false;
      }
      if (txCoordinator.isAlive(txState.getOriginator())) {
         //transaction started on another node but the node is still in the topology. 2 possible scenarios:
         // #1, the topology isn't updated
         // #2, the client timed-out waiting for the reply
         //in any case, we send a ignore reply and the client is free to retry (or rollback)
         writeNotExecuted(header);
         return true;
      }
      //originator is dead...

      //First phase state machine
      //success ACTIVE -> PREPARING -> PREPARED
      //failed ACTIVE -> MARK_ROLLBACK -> ROLLED_BACK or ACTIVE -> PREPARING -> ROLLED_BACK
      //1PC success ACTIVE -> PREPARING -> MARK_COMMIT -> COMMITTED
      switch (txState.getStatus()) {
         case ACTIVE:
         case PREPARING:
            //rollback existing transaction and retry with a new one
            txCoordinator.rollbackRemoteTransaction(txState.getGlobalTransaction());
            return false;
         case PREPARED:
            //2PC since 1PC never reaches this state
            writeResponse(header, createTransactionResponse(header, XAResource.XA_OK));
            return true;
         case MARK_ROLLBACK:
            //make sure it is rolled back and reply to the client
            txCoordinator.rollbackRemoteTransaction(txState.getGlobalTransaction());
         case ROLLED_BACK:
            writeResponse(header, createTransactionResponse(header, XAException.XA_RBROLLBACK));
            return true;
         case MARK_COMMIT:
            writeResponse(header, createTransactionResponse(header, txCoordinator.onePhaseCommitRemoteTransaction(txState.getGlobalTransaction(), txState.getModifications())));
            return true;
         case COMMITTED:
            writeResponse(header, createTransactionResponse(header, XAResource.XA_OK));
            return true;
         default:
            throw new IllegalStateException();
      }
   }

   /**
    * Validates if the value read is still valid and the write operation can proceed.
    */
   private boolean isValid(TransactionWrite write, AdvancedCache<byte[], byte[]> readCache) {
      if (write.skipRead()) {
         if (isTrace) {
            log.tracef("Operation %s wasn't read.", write);
         }
         return true;
      }
      CacheEntry<byte[], byte[]> entry = readCache.getCacheEntry(write.key);
      if (write.wasNonExisting()) {
         if (isTrace) {
            log.tracef("Key didn't exist for operation %s. Entry is %s", write, entry);
         }
         return entry == null || entry.getValue() == null;
      }
      if (isTrace) {
         log.tracef("Checking version for operation %s. Entry is %s", write, entry);
      }
      return entry != null && write.versionRead == MetadataUtils.extractVersion(entry);
   }

   private ByteBuf createTransactionResponse(HotRodHeader header, int xaReturnCode) {
      return header.encoder().transactionResponse(header, server, channel.alloc(), xaReturnCode);
   }

   private ByteBuf createRecoveryResponse(HotRodHeader header, Collection<Xid> xids) {
      return header.encoder().recoveryResponse(header, server, channel.alloc(), xids);
   }
}
