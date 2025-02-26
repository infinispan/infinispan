package org.infinispan.xsite;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.xsite.statetransfer.XSiteState;

/**
 * {@link org.infinispan.xsite.BackupReceiver} delegator. Mean to be overridden. For test purpose only!
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BackupReceiverDelegator implements BackupReceiver {

   protected final BackupReceiver delegate;

   protected BackupReceiverDelegator(BackupReceiver delegate) {
      if (delegate == null) {
         throw new NullPointerException("Delegate cannot be null");
      }
      this.delegate = delegate;
   }

   @Override
   public <O> CompletionStage<O> handleRemoteCommand(VisitableCommand command) {
      return delegate.handleRemoteCommand(command);
   }

   @Override
   public CompletionStage<Void> putKeyValue(Object key, Object value, Metadata metadata,
         IracMetadata iracMetadata) {
      return delegate.putKeyValue(key, value, metadata, iracMetadata);
   }

   @Override
   public CompletionStage<Void> removeKey(Object key, IracMetadata iracMetadata, boolean expiration) {
      return delegate.removeKey(key, iracMetadata, expiration);
   }

   @Override
   public CompletionStage<Void> clearKeys() {
      return delegate.clearKeys();
   }

   @Override
   public CompletionStage<Void> handleStateTransferControl(String originSite, boolean startReceiving) {
      return delegate.handleStateTransferControl(originSite, startReceiving);
   }

   @Override
   public CompletionStage<Void> handleStateTransferState(List<XSiteState> chunk, long timeoutMs) {
      return delegate.handleStateTransferState(chunk, timeoutMs);
   }

   @Override
   public CompletionStage<Boolean> touchEntry(Object key) {
      return delegate.touchEntry(key);
   }
}
