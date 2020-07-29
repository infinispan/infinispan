package org.infinispan.commands.irac;

import java.util.concurrent.CompletionStage;

import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;

public abstract class ForwardableCommand<O> extends XSiteReplicateCommand<O> {
   public ForwardableCommand(byte commandId, ByteString cacheName) {
      super(commandId, cacheName);
   }

   public abstract CompletionStage<O> executeOperation(BackupReceiver receiver);

   @Override
   public boolean isReturnValueExpected() {
      // If the type is
      return false;
   }

   public boolean isClear() {
      return false; //by default
   }

   public abstract ForwardableCommand<O> copyForCacheName(ByteString cacheName);

   public abstract ResponseCollector<O> responseCollector();
}
