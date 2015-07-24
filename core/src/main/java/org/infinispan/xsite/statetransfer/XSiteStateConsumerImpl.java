package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.infinispan.context.Flag.*;

/**
 * It contains the logic needed to consume the state sent from other site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateConsumerImpl implements XSiteStateConsumer {

   private static final EnumSet<Flag> STATE_TRANSFER_PUT_FLAGS = EnumSet.of(PUT_FOR_X_SITE_STATE_TRANSFER,
                                                                            IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP,
                                                                            SKIP_XSITE_BACKUP);
   private static final Log log = LogFactory.getLog(XSiteStateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();

   private TransactionManager transactionManager;
   private InvocationContextFactory invocationContextFactory;
   private CommandsFactory commandsFactory;
   private InterceptorChain interceptorChain;
   private CommitManager commitManager;
   private AtomicReference<String> sendingSite = new AtomicReference<>(null);

   @Inject
   public void inject(TransactionManager transactionManager, InvocationContextFactory invocationContextFactory,
                      CommandsFactory commandsFactory, InterceptorChain interceptorChain,
                      CommitManager commitManager) {
      this.transactionManager = transactionManager;
      this.invocationContextFactory = invocationContextFactory;
      this.commandsFactory = commandsFactory;
      this.interceptorChain = interceptorChain;
      this.commitManager = commitManager;
   }

   @Override
   public void startStateTransfer(String sendingSite) {
      if (debug) {
         log.debugf("Starting state transfer. Receiving from %s", sendingSite);
      }
      if (this.sendingSite.compareAndSet(null, sendingSite)) {
         commitManager.startTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      } else {
         throw new CacheException("Already receiving state from " + this.sendingSite.get());
      }
   }

   @Override
   public void endStateTransfer(String sendingSite) {
      if (debug) {
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
   public void applyState(XSiteState[] chunk) throws Exception {
      if (debug) {
         log.debugf("Received state: %s keys", chunk.length);
      }
      if (transactionManager != null) {
         applyStateInTransaction(chunk);
      } else {
         applyStateInNonTransaction(chunk);
      }
   }

   @Override
   public String getSendingSiteName() {
      return sendingSite.get();
   }

   private void applyStateInTransaction(XSiteState[] chunk) throws Exception {
      try {
         transactionManager.begin();
         InvocationContext ctx = invocationContextFactory.createInvocationContext(transactionManager.getTransaction(),
                                                                                  true);
         ((TxInvocationContext) ctx).getCacheTransaction().setStateTransferFlag(PUT_FOR_X_SITE_STATE_TRANSFER);
         for (XSiteState siteState : chunk) {
            interceptorChain.invoke(ctx, createPut(siteState));
            if (trace) {
               log.tracef("Successfully applied key'%s'", siteState);
            }
         }
         transactionManager.commit();
         if (debug) {
            log.debugf("Successfully applied state. %s keys inserted", chunk.length);
         }
      } catch (Exception e) {
         log.unableToApplyXSiteState(e);
         safeRollback();
         throw e;
      }
   }

   private void applyStateInNonTransaction(XSiteState[] chunk) {
      SingleKeyNonTxInvocationContext ctx = (SingleKeyNonTxInvocationContext) invocationContextFactory
            .createSingleKeyNonTxInvocationContext();

      for (XSiteState siteState : chunk) {
         PutKeyValueCommand command = createPut(siteState);
         ctx.setLockOwner(command.getLockOwner());
         interceptorChain.invoke(ctx, command);
         ctx.resetState(); //re-use same context. Old context is not longer needed
         if (trace) {
            log.tracef("Successfully applied key'%s'", siteState);
         }
      }
      if (debug) {
         log.debugf("Successfully applied state. %s keys inserted", chunk.length);
      }
   }

   private PutKeyValueCommand createPut(XSiteState state) {
      return commandsFactory.buildPutKeyValueCommand(state.key(), state.value(), state.metadata(),
                                                     STATE_TRANSFER_PUT_FLAGS);
   }

   private void safeRollback() {
      try {
         transactionManager.rollback();
      } catch (Exception e) {
         //ignored!
         if (debug) {
            log.debug("Error rollbacking transaction.", e);
         }
      }
   }
}
