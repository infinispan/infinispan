package org.infinispan.xsite;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.metadata.Metadata;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

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
   public Cache getCache() {
      return delegate.getCache();
   }

   @Override
   public CompletionStage<Void> handleRemoteCommand(VisitableCommand command, boolean preserveOrder) {
      return delegate.handleRemoteCommand(command, preserveOrder);
   }

   @Override
   public CompletionStage<Void> putKeyValue(Object key, Object value, Metadata metadata) {
      return delegate.putKeyValue(key, value, metadata);
   }

   @Override
   public CompletionStage<Void> removeKey(Object key) {
      return delegate.removeKey(key);
   }

   @Override
   public CompletionStage<Void> clearKeys() {
      return delegate.clearKeys();
   }

   @Override
   public CompletionStage<Void> handleStateTransferControl(XSiteStateTransferControlCommand command) {
      return delegate.handleStateTransferControl(command);
   }

   @Override
   public CompletionStage<Void> handleStateTransferState(XSiteStatePushCommand cmd) {
      return delegate.handleStateTransferState(cmd);
   }
}
