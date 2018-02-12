package org.infinispan.server.hotrod;

import java.util.concurrent.Executor;

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

import io.netty.channel.Channel;

class TransactionRequestProcessor extends CacheRequestProcessor {
   private static final Log log = LogFactory.getLog(TransactionRequestProcessor.class, Log.class);
   private static final boolean isTrace = log.isTraceEnabled();

   TransactionRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor, server);
   }

   /**
    * Handles a rollback request from a client.
    * @param cdc
    */
   void rollbackTransaction(CacheDecodeContext cdc) {
      validateConfiguration(cdc, cdc.cache());
      executor.execute(() -> rollbackTransactionInternal(cdc));
   }

   private void rollbackTransactionInternal(CacheDecodeContext cdc) {
      try {
         writeResponse(finishTransaction(cdc, new RollbackTransactionDecodeContext(cdc.cache(), (XidImpl) cdc.operationDecodeContext)));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   /**
    * Handles a prepare request from a client
    * @param cdc
    */
   void prepareTransaction(CacheDecodeContext cdc) {
      validateConfiguration(cdc, cdc.cache());
      executor.execute(() -> prepareTransactionInternal(cdc));
   }

   private void prepareTransactionInternal(CacheDecodeContext cdc) {
      try {
         AdvancedCache<byte[], byte[]> cache = cdc.cache();
         PrepareTransactionContext context = (PrepareTransactionContext) cdc.operationDecodeContext;
         if (context.isEmpty()) {
            //the client can optimize and avoid contacting the server when no data is written.
            if (isTrace) {
               log.tracef("Transaction %s is read only.", context.getXid());
            }
            writeResponse(createTransactionResponse(cdc.header, XAResource.XA_RDONLY));
            return;
         }
         PrepareTransactionDecodeContext txContext = new PrepareTransactionDecodeContext(cache, context.getXid());

         Response response = checkExistingTxForPrepare(cdc, txContext);
         if (response != null) {
            if (isTrace) {
               log.tracef("Transaction %s conflicts with another node. Response is %s", context.getXid(), response);
            }
            writeResponse(response);
            return;
         }

         if (!txContext.startTransaction()) {
            if (isTrace) {
               log.tracef("Unable to start transaction %s", context.getXid());
            }
            writeResponse(cdc.decoder.createNotExecutedResponse(cdc.header, null));
            return;
         }

         //forces the write-lock. used by pessimistic transaction. it ensures the key is not written after is it read and validated
         //optimistic transaction will use the write-skew check.
         AdvancedCache<byte[], byte[]> txCache = txContext.decorateCache(cache);

         try {
            for (TransactionWrite write : context.writes()) {
               if (isValid(write, txCache)) {
                  if (write.isRemove()) {
                     txCache.remove(write.key);
                  } else {
                     txCache.put(write.key, write.value, cdc.buildMetadata(write.lifespan, write.maxIdle));
                  }
               } else {
                  txContext.setRollbackOnly();
                  break;
               }
            }
            int xaCode = txContext.prepare(context.isOnePhaseCommit());
            writeResponse(createTransactionResponse(cdc.header, xaCode));
         } catch (Exception e) {
            writeResponse(createTransactionResponse(cdc.header, txContext.rollback()));
         } finally {
            EmbeddedTransactionManager.dissociateTransaction();
         }
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   /**
    * Handles a commit request from a client
    * @param cdc
    */
   void commitTransaction(CacheDecodeContext cdc) {
      validateConfiguration(cdc, cdc.cache());
      executor.execute(() -> commitTransactionInternal(cdc));
   }

   private void commitTransactionInternal(CacheDecodeContext cdc) {
      try {
         writeResponse(finishTransaction(cdc, new CommitTransactionDecodeContext(cdc.cache(), (XidImpl) cdc.operationDecodeContext)));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   /**
    * Commits or Rollbacks the transaction (second phase of two-phase-commit)
    */
   private TransactionResponse finishTransaction(CacheDecodeContext cdc, SecondPhaseTransactionDecodeContext txContext) {
      try {
         txContext.perform();
      } catch (HeuristicMixedException e) {
         return createTransactionResponse(cdc.header, XAException.XA_HEURMIX);
      } catch (HeuristicRollbackException e) {
         return createTransactionResponse(cdc.header, XAException.XA_HEURRB);
      } catch (RollbackException e) {
         return createTransactionResponse(cdc.header, XAException.XA_RBROLLBACK);
      }
      return createTransactionResponse(cdc.header, XAResource.XA_OK);
   }

   /**
    * Checks if the configuration (and the transaction manager) is able to handle client transactions.
    * @param cdc
    * @param cache
    */
   private void validateConfiguration(CacheDecodeContext cdc, AdvancedCache<byte[], byte[]> cache) {
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
   private Response checkExistingTxForPrepare(CacheDecodeContext cdc, PrepareTransactionDecodeContext context) {
      TxState txState = context.getTxState();
      if (txState == null) {
         return null;
      }
      switch (txState.status()) {
         case Status.STATUS_ACTIVE:
            break;
         case Status.STATUS_PREPARED:
            return createTransactionResponse(cdc.header, XAResource.XA_OK);
         case Status.STATUS_ROLLEDBACK:
            return createTransactionResponse(cdc.header, XAException.XA_RBROLLBACK);
         case Status.STATUS_COMMITTED:
            //weird case. the tx is committed but we received a prepare request?
            return createTransactionResponse(cdc.header, XAResource.XA_OK);
         default:
            throw new IllegalStateException();
      }
      if (context.isAlive(txState.getOriginator())) {
         //transaction started on another node but the node is still in the topology. 2 possible scenarios:
         // #1, the topology isn't updated
         // #2, the client timed-out waiting for the reply
         //in any case, we send a ignore reply and the client is free to retry (or rollback)
         return cdc.decoder.createNotExecutedResponse(cdc.header, null);
      } else {
         //node left the cluster while transaction was running or preparing. we are going to abort the other transaction and start a new one.
         context.rollbackRemoteTransaction();
      }
      return null;
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

   /**
    * Creates a transaction response with the specific xa-code.
    */
   private TransactionResponse createTransactionResponse(HotRodHeader header, int xaReturnCode) {
      return new TransactionResponse(header.version, header.messageId, header.cacheName, header.clientIntel, header.op, OperationStatus.Success, header.topologyId, xaReturnCode);
   }
}
