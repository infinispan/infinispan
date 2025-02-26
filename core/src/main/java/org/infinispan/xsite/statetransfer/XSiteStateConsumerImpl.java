package org.infinispan.xsite.statetransfer;

import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.IRAC_STATE;
import static org.infinispan.context.Flag.PUT_FOR_X_SITE_STATE_TRANSFER;
import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import static org.infinispan.context.Flag.SKIP_XSITE_BACKUP;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.tx.TransactionImpl;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.SingleKeyNonTxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * It contains the logic needed to consume the state sent from other site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Scope(Scopes.NAMED_CACHE)
public class XSiteStateConsumerImpl implements XSiteStateConsumer {

   private static final long STATE_TRANSFER_PUT_FLAGS = EnumUtil.bitSetOf(PUT_FOR_X_SITE_STATE_TRANSFER,
                                                                          IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP,
                                                                          SKIP_XSITE_BACKUP, IRAC_STATE);
   private static final Log log = LogFactory.getLog(XSiteStateConsumerImpl.class);
   private static final AtomicLong TX_ID_GENERATOR = new AtomicLong(0);

   @Inject TransactionTable transactionTable;
   @Inject InvocationContextFactory invocationContextFactory;
   @Inject CommandsFactory commandsFactory;
   @Inject AsyncInterceptorChain interceptorChain;
   @Inject CommitManager commitManager;
   @Inject KeyPartitioner keyPartitioner;

   private final boolean isTxVersioned;
   private final boolean isTransactional;

   private final AtomicReference<String> sendingSite = new AtomicReference<>(null);

   public XSiteStateConsumerImpl(Configuration configuration) {
      this.isTxVersioned = Configurations.isTxVersioned(configuration);
      this.isTransactional = configuration.transaction().transactionMode().isTransactional();
   }

   @Override
   public void startStateTransfer(String sendingSite) {
      log.debugf("Starting state transfer. Receiving from %s", sendingSite);
      if (this.sendingSite.compareAndSet(null, sendingSite)) {
         commitManager.startTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      } else {
         throw new CacheException("Already receiving state from " + this.sendingSite.get());
      }
   }

   @Override
   public void endStateTransfer(String sendingSite) {
      if (log.isDebugEnabled()) {
         log.debugf("Ending state transfer from %s", sendingSite);
      }
      String currentSendingSite = this.sendingSite.get();
      if (sendingSite == null || sendingSite.equals(currentSendingSite)) {
         this.sendingSite.set(null);
         commitManager.stopTrack(PUT_FOR_X_SITE_STATE_TRANSFER);
      } else {
         if (log.isDebugEnabled()) {
            log.debugf("Received an end request from a non-sender site. Expects %s but got %s", currentSendingSite,
                       sendingSite);
         }
      }
   }

   @Override
   public void applyState(List<XSiteState> chunk) throws Exception {
      if (log.isDebugEnabled()) {
         log.debugf("Received state: %s keys", chunk.size());
      }
      if (isTransactional) {
         applyStateInTransaction(chunk);
      } else {
         applyStateInNonTransaction(chunk);
      }
   }

   @Override
   public String getSendingSiteName() {
      return sendingSite.get();
   }

   private void applyStateInTransaction(List<XSiteState> chunk) {
      XSiteApplyStateTransaction tx = new XSiteApplyStateTransaction();
      InvocationContext ctx = invocationContextFactory.createInvocationContext(tx, false);
      assert ctx instanceof LocalTxInvocationContext;
      LocalTransaction localTransaction = ((LocalTxInvocationContext) ctx).getCacheTransaction();

      try {
         localTransaction.setStateTransferFlag(PUT_FOR_X_SITE_STATE_TRANSFER);
         for (XSiteState siteState : chunk) {
            interceptorChain.invoke(ctx, createPut(siteState));
            if (log.isTraceEnabled()) {
               log.tracef("Successfully applied key'%s'", siteState);
            }
         }
         invoke1PCPrepare(localTransaction);
         if (log.isDebugEnabled()) {
            log.debugf("Successfully applied state. %s keys inserted", chunk.size());
         }
      } catch (Exception e) {
         log.unableToApplyXSiteState(e);
         safeRollback(localTransaction);
         throw e;
      } finally {
         transactionTable.removeLocalTransaction(localTransaction);
      }
   }

   private void applyStateInNonTransaction(List<XSiteState> chunk) {
      SingleKeyNonTxInvocationContext ctx = (SingleKeyNonTxInvocationContext) invocationContextFactory
            .createSingleKeyNonTxInvocationContext();

      for (XSiteState siteState : chunk) {
         PutKeyValueCommand command = createPut(siteState);
         ctx.setLockOwner(command.getKeyLockOwner());
         interceptorChain.invoke(ctx, command);
         ctx.resetState(); //re-use same context. Old context is not longer needed
         if (log.isTraceEnabled()) {
            log.tracef("Successfully applied key'%s'", siteState);
         }
      }
      if (log.isDebugEnabled()) {
         log.debugf("Successfully applied state. %s keys inserted", chunk.size());
      }
   }

   private PutKeyValueCommand createPut(XSiteState state) {
      Object key = state.key();
      PutKeyValueCommand cmd = commandsFactory.buildPutKeyValueCommand(key, state.value(),
            keyPartitioner.getSegment(key), state.metadata(), STATE_TRANSFER_PUT_FLAGS);
      cmd.setInternalMetadata(state.internalMetadata());
      return cmd;
   }

   private void invoke1PCPrepare(LocalTransaction localTransaction) {
      PrepareCommand prepareCommand;
      if (isTxVersioned) {
         prepareCommand = commandsFactory.buildVersionedPrepareCommand(localTransaction.getGlobalTransaction(),
               localTransaction.getModifications(), true);
      } else {
         prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(),
               localTransaction.getModifications(), true);
      }
      LocalTxInvocationContext ctx = invocationContextFactory.createTxInvocationContext(localTransaction);
      interceptorChain.invoke(ctx, prepareCommand);
   }

   private void safeRollback(LocalTransaction localTransaction) {
      try {
         RollbackCommand prepareCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
         LocalTxInvocationContext ctx = invocationContextFactory.createTxInvocationContext(localTransaction);
         interceptorChain.invokeAsync(ctx, prepareCommand);
      } catch (Exception e) {
         //ignored!
         if (log.isDebugEnabled()) {
            log.debug("Error rollbacking transaction.", e);
         }
      }
   }

   private static class XSiteApplyStateTransaction extends TransactionImpl {
      // Make it different from embedded txs (1)
      // Make it different from embedded state transfer txs (2)
      static final int FORMAT_ID = 3;


      XSiteApplyStateTransaction() {
         byte[] bytes = new byte[8];
         Util.longToBytes(TX_ID_GENERATOR.incrementAndGet(), bytes, 0);
         XidImpl xid = XidImpl.create(FORMAT_ID, bytes, bytes);
         setXid(xid);
      }
   }
}
