package org.infinispan.commands.irac;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.irac.IracManager;

/**
 * An update request that is sent to the remote site by {@link IracManager}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public abstract class IracUpdateKeyCommand extends XSiteReplicateCommand {

   protected IracUpdateKeyCommand(byte commandId, ByteString cacheName) {
      super(commandId, cacheName);
   }

   @Override
   public final CompletionStage<Void> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      assert !preserveOrder : "IRAC Update Command sent asynchronously!";
      return receiver.forwardToPrimary(this);
   }


   @Override
   public final CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      //noinspection unchecked
      Cache<Object, Object> cache = registry.getCache().running();
      //TODO! create a component for BackupReceiver
      // See https://issues.redhat.com/browse/ISPN-11800
      BackupReceiver backupReceiver = registry.getGlobalComponentRegistry()
            .getComponent(BackupReceiverRepository.class)
            .getBackupReceiver(cache);
      return executeOperation(backupReceiver);
   }

   @Override
   public final boolean isReturnValueExpected() {
      return false;
   }

   public abstract Object getKey();

   public abstract CompletionStage<Void> executeOperation(BackupReceiver receiver);

   public abstract IracUpdateKeyCommand copyForCacheName(ByteString cacheName);

   public boolean isClear() {
      return false; //by default
   }
}
