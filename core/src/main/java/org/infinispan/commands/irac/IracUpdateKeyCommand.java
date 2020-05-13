package org.infinispan.commands.irac;

import java.util.concurrent.CompletionStage;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
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
      return executeOperation(registry.getBackupReceiver().running());
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
