package org.infinispan.xsite.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.irac.IracManager;

/**
 * An update request that is sent to the remote site by {@link IracManager}.
 *
 * @author Pedro Ruivo
 * @since 15.0
 */
public abstract class IracUpdateKeyRequest<T> extends XSiteCacheRequest<T> {

   protected IracUpdateKeyRequest(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   protected CompletionStage<T> invokeInLocalCache(String origin, ComponentRegistry registry) {
      return executeOperation(registry.getBackupReceiver().running());
   }

   public abstract CompletionStage<T> executeOperation(BackupReceiver receiver);

}
