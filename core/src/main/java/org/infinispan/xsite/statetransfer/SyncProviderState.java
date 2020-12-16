package org.infinispan.xsite.statetransfer;

import static org.infinispan.util.logging.Log.XSITE;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableObserver;
import io.reactivex.rxjava3.core.CompletableSource;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.functions.Predicate;
import net.jcip.annotations.GuardedBy;

/**
 * A {@link XSiteStateProviderState} for synchronous cross-site replication state transfer.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class SyncProviderState implements XSiteStateProviderState {

   private static final Log log = LogFactory.getLog(SyncProviderState.class);
   private static final AtomicReferenceFieldUpdater<SyncProviderState, OutboundTask> TASK_UPDATER = AtomicReferenceFieldUpdater.newUpdater(SyncProviderState.class, OutboundTask.class, "task");

   private final XSiteBackup backup;
   private final XSiteStateTransferConfiguration configuration;
   private volatile OutboundTask task;

   private SyncProviderState(XSiteBackup backup, XSiteStateTransferConfiguration configuration) {
      this.backup = backup;
      this.configuration = configuration;
   }

   public static SyncProviderState create(BackupConfiguration config) {
      XSiteBackup backup = new XSiteBackup(config.site(), true, config.stateTransfer().timeout());
      return new SyncProviderState(backup, config.stateTransfer());
   }

   @Override
   public XSiteStatePushTask createPushTask(Address originator, XSiteStateProvider provider) {
      OutboundTask newTask = new OutboundTask(originator, provider, this);
      return TASK_UPDATER.compareAndSet(this, null, newTask) ? newTask : null;
   }

   @Override
   public void cancelTransfer() {
      OutboundTask currentTask = TASK_UPDATER.getAndSet(this, null);
      if (currentTask != null) {
         currentTask.cancel();
      }
   }

   @Override
   public boolean isSending() {
      return task != null;
   }

   @Override
   public boolean isOriginatorMissing(Collection<Address> members) {
      OutboundTask currentTask = task;
      return currentTask != null && !members.contains(currentTask.coordinator);
   }

   // methods for OutboundTask
   void taskFinished() {
      TASK_UPDATER.set(this, null);
   }

   XSiteBackup getBackup() {
      return backup;
   }

   int getChunkSize() {
      return configuration.chunkSize();
   }

   long getWaitTimeMillis() {
      return configuration.waitTime();
   }

   int getMaxRetries() {
      return configuration.maxRetries();
   }

   private static class OutboundTask implements XSiteStatePushTask, Predicate<List<XSiteState>>, Function<List<XSiteState>, CompletableSource>, CompletableObserver {

      private final Address coordinator;
      private final XSiteStateProvider provider;
      private final SyncProviderState state;
      private volatile boolean canceled = false;

      private OutboundTask(Address coordinator, XSiteStateProvider provider, SyncProviderState state) {
         this.coordinator = coordinator;
         this.provider = provider;
         this.state = state;
      }

      @Override
      public void execute(Flowable<XSiteState> flowable, CompletionStage<Void> delayer) {
         //delayer is the cache topology future. we need to ensure the topology id is installed before iterating
         delayer.thenRunAsync(() -> flowable
                     .buffer(state.getChunkSize())
                     .takeUntil(this)
                     .concatMapCompletable(this, 1)
                     .subscribe(this),
               provider.getExecutor());

      }

      public void cancel() {
         canceled = true;
      }

      @Override
      public boolean test(List<XSiteState> ignored) {
         //Flowable#takeUntil method
         return canceled;
      }

      @Override
      public void onSubscribe(@NonNull Disposable d) {

      }

      @Override
      public void onComplete() {
         //if canceled, the coordinator already cleanup the resources. There is nothing to be done here.
         if (canceled) {
            return;
         }
         provider.notifyStateTransferEnd(state.getBackup().getSiteName(), coordinator, true);
         state.taskFinished();
      }

      @Override
      public void onError(@NonNull Throwable e) {
         //if canceled, the coordinator already cleanup the resources. There is nothing to be done here.
         if (canceled) {
            return;
         }
         provider.notifyStateTransferEnd(state.getBackup().getSiteName(), coordinator, false);
         state.taskFinished();
      }

      @Override
      public CompletableSource apply(List<XSiteState> xSiteStates) {
         //Flowable#concatMapCompletable method
         XSiteBackup backup = state.getBackup();
         //TODO!? can we use xSiteStates directly instead of copying?
         XSiteState[] privateBuffer = xSiteStates.toArray(new XSiteState[0]);

         if (log.isDebugEnabled()) {
            log.debugf("Sending chunk to site '%s'. Chunk has %s keys.", backup.getSiteName(), privateBuffer.length);
         }

         XSiteStatePushCommand command = provider.getCommandsFactory().buildXSiteStatePushCommand(privateBuffer, backup.getTimeout());
         return Completable.fromCompletionStage(new CommandRetry(backup, command, provider, state.getWaitTimeMillis(), state.getMaxRetries()).send());
      }
   }

   private static class CommandRetry extends CompletableFuture<Void> implements java.util.function.BiConsumer<Void, Throwable> {

      private final XSiteBackup backup;
      private final XSiteStatePushCommand cmd;
      private final XSiteStateProvider provider;
      private final long waitTimeMillis;
      @GuardedBy("this")
      private int maxRetries;

      private CommandRetry(XSiteBackup backup, XSiteStatePushCommand cmd, XSiteStateProvider provider, long waitTimeMillis, int maxRetries) {
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
