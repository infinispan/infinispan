package org.infinispan.server.hotrod;

import java.util.List;
import java.util.concurrent.Executor;

import javax.security.auth.Subject;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.CommitTransactionDecodeContext;
import org.infinispan.server.hotrod.tx.PrepareTransactionDecodeContext;
import org.infinispan.server.hotrod.tx.RollbackTransactionDecodeContext;
import org.infinispan.server.hotrod.tx.SecondPhaseTransactionDecodeContext;
import org.infinispan.server.hotrod.tx.TxState;
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

   /**
    * Handles a rollback request from a client.
    * @param header
    * @param subject
    * @param xid
    */
   void rollbackTransaction(HotRodHeader header, Subject subject, XidImpl xid) {
      AdvancedCache<byte[], byte[]> cache = server.cache(header, subject);
      validateConfiguration(cache);
      executor.execute(() -> rollbackTransactionInternal(header, cache, xid));
   }

   private void rollbackTransactionInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, XidImpl xid) {
      try {
         writeResponse(header, finishTransaction(header, new RollbackTransactionDecodeContext(cache, xid)));
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   /**
    * Handles a prepare request from a client
    * @param header
    * @param subject
    * @param xid
    * @param onePhaseCommit
    * @param writes
    */
   void prepareTransaction(HotRodHeader header, Subject subject, XidImpl xid, boolean onePhaseCommit, List<TransactionWrite> writes) {
      AdvancedCache<byte[], byte[]> cache = server.cache(header, subject);
      validateConfiguration(cache);
      executor.execute(() -> prepareTransactionInternal(header, cache, xid, onePhaseCommit, writes));
   }

   private void prepareTransactionInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, XidImpl xid, boolean onePhaseCommit, List<TransactionWrite> writes) {
      try {
         if (writes.isEmpty()) {
            //the client can optimize and avoid contacting the server when no data is written.
            if (isTrace) {
               log.tracef("Transaction %s is read only.", xid);
            }
            writeResponse(header, createTransactionResponse(header, XAResource.XA_RDONLY));
            return;
         }
         PrepareTransactionDecodeContext txContext = new PrepareTransactionDecodeContext(cache, xid);

         if (checkExistingTxForPrepare(header, txContext)) {
            if (isTrace) {
               log.tracef("Transaction %s conflicts with another node.", xid);
            }
            return;
         }

         if (!txContext.startTransaction()) {
            if (isTrace) {
               log.tracef("Unable to start transaction %s", xid);
            }
            writeNotExecuted(header);
            return;
         }

         //forces the write-lock. used by pessimistic transaction. it ensures the key is not written after is it read and validated
         //optimistic transaction will use the write-skew check.
         AdvancedCache<byte[], byte[]> txCache = txContext.decorateCache(cache);

         try {
            for (TransactionWrite write : writes) {
               if (isValid(write, txCache)) {
                  if (write.isRemove()) {
                     txCache.remove(write.key);
                  } else {
                     txCache.put(write.key, write.value, write.metadata);
                  }
               } else {
                  txContext.setRollbackOnly();
                  break;
               }
            }
            int xaCode = txContext.prepare(onePhaseCommit);
            writeResponse(header, createTransactionResponse(header, xaCode));
         } catch (Exception e) {
            writeResponse(header, createTransactionResponse(header, txContext.rollback()));
         } finally {
            EmbeddedTransactionManager.dissociateTransaction();
         }
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   /**
    * Handles a commit request from a client
    * @param header
    * @param subject
    * @param xid
    */
   void commitTransaction(HotRodHeader header, Subject subject, XidImpl xid) {
      AdvancedCache<byte[], byte[]> cache = server.cache(header, subject);
      validateConfiguration(cache);
      executor.execute(() -> commitTransactionInternal(header, cache, xid));
   }

   private void commitTransactionInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, XidImpl xid) {
      try {
         writeResponse(header, finishTransaction(header, new CommitTransactionDecodeContext(cache, xid)));
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   /**
    * Commits or Rollbacks the transaction (second phase of two-phase-commit)
    */
   private ByteBuf finishTransaction(HotRodHeader header, SecondPhaseTransactionDecodeContext txContext) {
      try {
         txContext.perform();
      } catch (HeuristicMixedException e) {
         return createTransactionResponse(header, XAException.XA_HEURMIX);
      } catch (HeuristicRollbackException e) {
         return createTransactionResponse(header, XAException.XA_HEURRB);
      } catch (RollbackException e) {
         return createTransactionResponse(header, XAException.XA_RBROLLBACK);
      }
      return createTransactionResponse(header, XAResource.XA_OK);
   }

   /**
    * Checks if the configuration (and the transaction manager) is able to handle client transactions.
    * @param cache
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
         //no Log. see TODO.
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
   private boolean checkExistingTxForPrepare(HotRodHeader header, PrepareTransactionDecodeContext context) {
      TxState txState = context.getTxState();
      if (txState == null) {
         return false;
      }
      switch (txState.status()) {
         case Status.STATUS_ACTIVE:
            break;
         case Status.STATUS_PREPARED:
            writeResponse(header, createTransactionResponse(header, XAResource.XA_OK));
            return true;
         case Status.STATUS_ROLLEDBACK:
            writeResponse(header, createTransactionResponse(header, XAException.XA_RBROLLBACK));
            return true;
         case Status.STATUS_COMMITTED:
            //weird case. the tx is committed but we received a prepare request?
            writeResponse(header, createTransactionResponse(header, XAResource.XA_OK));
            return true;
         default:
            throw new IllegalStateException();
      }
      if (context.isAlive(txState.getOriginator())) {
         //transaction started on another node but the node is still in the topology. 2 possible scenarios:
         // #1, the topology isn't updated
         // #2, the client timed-out waiting for the reply
         //in any case, we send a ignore reply and the client is free to retry (or rollback)
         writeNotExecuted(header);
         return true;
      } else {
         //node left the cluster while transaction was running or preparing. we are going to abort the other transaction and start a new one.
         context.rollbackRemoteTransaction();
         return false;
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
}
