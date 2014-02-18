package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

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
   public void startStateTransfer() {
      if (debug) {
         log.debugf("Starting state transfer.");
      }
      commitManager.startTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
   }

   @Override
   public void endStateTransfer() {
      if (debug) {
         log.debugf("Ending state transfer.");
      }
      commitManager.stopTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
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
         interceptorChain.invoke(ctx, createPut(siteState));
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

   private Collection<Object> extractKeys(XSiteState[] chunk) {
      List<Object> keys = new ArrayList<Object>(chunk.length);
      for (XSiteState aChunk : chunk) {
         keys.add(aChunk.key());
      }
      return keys;
   }
}
