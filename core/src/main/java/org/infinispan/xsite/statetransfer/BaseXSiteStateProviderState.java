package org.infinispan.xsite.statetransfer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.xsite.XSiteBackup;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.CompletableObserver;
import io.reactivex.rxjava3.core.CompletableSource;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.functions.Predicate;

/**
 * Common code for {@link AsyncProviderState} and {@link SyncProviderState} implementation.
 * <p>
 * The only difference between the two implementation is the way the state is send to the remote site. The synchronous
 * implementation sends the state directly while the asynchronous makes use of IRAC (and its conflict resolution).
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public abstract class BaseXSiteStateProviderState<T extends BaseXSiteStateProviderState.OutboundTask> implements XSiteStateProviderState {

   private final XSiteBackup backup;
   private final XSiteStateTransferConfiguration configuration;
   private final AtomicReference<T> task;

   public BaseXSiteStateProviderState(XSiteBackup backup, XSiteStateTransferConfiguration configuration) {
      this.backup = backup;
      this.configuration = configuration;
      task = new AtomicReference<>();
   }

   @Override
   public XSiteStatePushTask createPushTask(Address originator, XSiteStateProvider provider) {
      T newTask = createTask(originator, provider);
      return task.compareAndSet(null, newTask) ? newTask : null;
   }


   @Override
   public void cancelTransfer() {
      T currentTask = task.getAndSet(null);
      if (currentTask != null) {
         currentTask.cancel();
      }
   }

   @Override
   public boolean isSending() {
      return task.get() != null;
   }

   @Override
   public boolean isOriginatorMissing(Collection<Address> members) {
      T currentTask = task.get();
      return currentTask != null && !members.contains(currentTask.getCoordinator());
   }

   // methods for OutboundTask
   void taskFinished() {
      task.set(null);
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

   abstract T createTask(Address originator, XSiteStateProvider provider);

   abstract static class OutboundTask implements XSiteStatePushTask, Predicate<List<XSiteState>>, Function<List<XSiteState>, CompletableSource>, CompletableObserver {

      private final Address coordinator;
      final XSiteStateProvider provider;
      final BaseXSiteStateProviderState<?> state;
      private volatile boolean canceled = false;

      OutboundTask(Address coordinator, XSiteStateProvider provider, BaseXSiteStateProviderState<?> state) {
         this.coordinator = coordinator;
         this.provider = provider;
         this.state = state;
      }

      Address getCoordinator() {
         return coordinator;
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
   }
}
