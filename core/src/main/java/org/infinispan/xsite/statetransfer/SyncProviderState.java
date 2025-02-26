package org.infinispan.xsite.statetransfer;

import static org.infinispan.util.logging.Log.XSITE;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableSource;
import net.jcip.annotations.GuardedBy;
import org.infinispan.xsite.commands.remote.XSiteStatePushRequest;

/**
 * A {@link XSiteStateProviderState} for synchronous cross-site replication state transfer.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class SyncProviderState extends BaseXSiteStateProviderState<SyncProviderState.SyncOutboundTask> {

   private static final Log log = LogFactory.getLog(SyncProviderState.class);

   private SyncProviderState(XSiteBackup backup, XSiteStateTransferConfiguration configuration) {
      super(backup, configuration);
   }

   public static SyncProviderState create(BackupConfiguration config) {
      XSiteBackup backup = new XSiteBackup(config.site(), true, config.stateTransfer().timeout());
      return new SyncProviderState(backup, config.stateTransfer());
   }

   @Override
   public boolean isSync() {
      return true;
   }

   @Override
   SyncOutboundTask createTask(Address originator, XSiteStateProvider provider) {
      return new SyncOutboundTask(originator, provider, this);
   }

   static class SyncOutboundTask extends BaseXSiteStateProviderState.OutboundTask {

      SyncOutboundTask(Address coordinator, XSiteStateProvider provider, SyncProviderState state) {
         super(coordinator, provider, state);
      }

      @Override
      public CompletableSource apply(List<XSiteState> xSiteStates) {
         //Flowable#concatMapCompletable method
         XSiteBackup backup = state.getBackup();

         if (log.isDebugEnabled()) {
            log.debugf("Sending chunk to site '%s'. Chunk has %s keys.", backup.getSiteName(), xSiteStates.size());
         }

         XSiteStatePushRequest command = provider.getCommandsFactory().buildXSiteStatePushRequest(xSiteStates, backup.getTimeout());
         return Completable.fromCompletionStage(new CommandRetry(backup, command, provider, state.getWaitTimeMillis(), state.getMaxRetries()).send());
      }
   }

   private static class CommandRetry extends CompletableFuture<Void> implements java.util.function.BiConsumer<Void, Throwable> {

      private final XSiteBackup backup;
      private final XSiteStatePushRequest cmd;
      private final XSiteStateProvider provider;
      private final long waitTimeMillis;
      @GuardedBy("this")
      private int maxRetries;

      private CommandRetry(XSiteBackup backup, XSiteStatePushRequest cmd, XSiteStateProvider provider, long waitTimeMillis, int maxRetries) {
         this.backup = backup;
         this.cmd = cmd;
         this.provider = provider;
         this.waitTimeMillis = waitTimeMillis;
         this.maxRetries = maxRetries;
      }

      //method to invoke from other class
      CompletionStage<Void> send() {
         doSend();
         return this;
      }

      //used in scheduled executor, invokes doSend after the wait time.
      void nonBlockingSend() {
         provider.getExecutor().execute(this::doSend);
      }

      //actual send
      private void doSend() {
         provider.getRpcManager().invokeXSite(backup, cmd).whenComplete(this);
      }

      @Override
      public void accept(Void o, Throwable throwable) {
         //CompletionStage#whenComplete method, with the reply from remote site.
         if (throwable != null) {
            if (canRetry()) {
               if (log.isTraceEnabled()) {
                  log.tracef("Command %s is going to be retried.", cmd);
               }
               if (waitTimeMillis <= 0) {
                  send();
               } else {
                  provider.getScheduledExecutorService().schedule(this::nonBlockingSend, waitTimeMillis, TimeUnit.MILLISECONDS);
               }
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef("Command %s failed.", cmd);
               }
               Throwable cause = CompletableFutures.extractException(throwable);
               XSITE.unableToSendXSiteState(backup.getSiteName(), cause);
               completeExceptionally(cause);
            }
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Command %s successful.", cmd);
            }
            complete(null);
         }
      }

      private synchronized boolean canRetry() {
         return maxRetries-- > 0;
      }
   }
}
