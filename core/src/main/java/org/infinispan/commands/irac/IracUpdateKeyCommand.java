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
public abstract class IracUpdateKeyCommand<T> extends XSiteReplicateCommand<T> {

   protected IracUpdateKeyCommand(byte commandId, ByteString cacheName) {
      super(commandId, cacheName);
   }

   @Override
   public final CompletionStage<T> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      assert !preserveOrder : "IRAC Update Command sent asynchronously!";
      return executeOperation(receiver);
   }

   @Override
   public final CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      throw new IllegalStateException(); //should never be invoked.
   }

   @Override
   public final boolean isReturnValueExpected() {
      return false;
   }

   public abstract CompletionStage<T> executeOperation(BackupReceiver receiver);

   public boolean isClear() {
      return false; //by default
   }
}
