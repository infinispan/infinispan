package org.infinispan.xsite.statetransfer;

import java.util.List;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableSource;

/**
 * A {@link XSiteStateProviderState} for asynchronous cross-site replication state transfer (IRAC).
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class AsyncProviderState extends BaseXSiteStateProviderState<AsyncProviderState.AsyncOutboundTask> {

   private static final Log log = LogFactory.getLog(AsyncProviderState.class);

   private AsyncProviderState(XSiteBackup backup, XSiteStateTransferConfiguration configuration) {
      super(backup, configuration);
   }

   public static AsyncProviderState create(BackupConfiguration config) {
      XSiteBackup backup = new XSiteBackup(config.site(), true, config.stateTransfer().timeout());
      return new AsyncProviderState(backup, config.stateTransfer());
   }

   @Override
   public boolean isSync() {
      return false;
   }

   @Override
   AsyncOutboundTask createTask(Address originator, XSiteStateProvider provider) {
      return new AsyncOutboundTask(originator, provider, this);
   }

   static class AsyncOutboundTask extends BaseXSiteStateProviderState.OutboundTask {

      AsyncOutboundTask(Address coordinator, XSiteStateProvider provider, AsyncProviderState state) {
         super(coordinator, provider, state);
      }

      @Override
      public CompletableSource apply(List<XSiteState> xSiteStates) {
         //Flowable#concatMapCompletable method
         if (log.isDebugEnabled()) {
            log.debugf("Sending chunk to site '%s'. Chunk has %s keys.", state.getBackup().getSiteName(), xSiteStates.size());
         }
         return Completable.fromCompletionStage(provider.getIracManager().trackForStateTransfer(xSiteStates));
      }
   }

}
