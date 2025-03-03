package org.infinispan.server.hotrod.command.tx;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.hotrod.tx.operation.Util;
import org.infinispan.util.ByteString;

/**
 * A {@link CacheRpcCommand} implementation to forward the commit request from a client to the member that run the
 * transaction.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_TX_FORWARD_COMMIT_COMMAND)
public class ForwardCommitCommand extends AbstractForwardTxCommand {

   @ProtoFactory
   public ForwardCommitCommand(ByteString cacheName, XidImpl xid, long timeout) {
      super(cacheName, xid, timeout);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      Util.commitLocalTransaction(componentRegistry.getCache().wired(), xid, timeout);
      return CompletableFutures.completedNull();
   }

   @Override
   public String toString() {
      return "ForwardCommitCommand{" +
            "cacheName=" + cacheName +
            ", xid=" + xid +
            '}';
   }
}
