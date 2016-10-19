package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Set;

import org.infinispan.commands.CancelCommand;
import org.infinispan.commands.CreateCacheCommand;
import org.infinispan.commands.RemoveCacheCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderRollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.DelegatingObjectInput;
import org.infinispan.commons.marshall.DelegatingObjectOutput;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.stream.impl.StreamRequestCommand;
import org.infinispan.stream.impl.StreamResponseCommand;
import org.infinispan.stream.impl.StreamSegmentResponseCommand;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.XSiteAdminCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

/**
 * Externalizer in charge of marshalling cache specific commands. At read time,
 * this marshaller is able to locate the right cache marshaller and provide
 * it any externalizers implementations that follow.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public final class CacheRpcCommandExternalizer extends AbstractExternalizer<CacheRpcCommand> {
   private final GlobalComponentRegistry gcr;
   private final ReplicableCommandExternalizer cmdExt;
   private final StreamingMarshaller globalMarshaller;

   public CacheRpcCommandExternalizer(GlobalComponentRegistry gcr, ReplicableCommandExternalizer cmdExt) {
      this.cmdExt = cmdExt;
      this.gcr = gcr;
      //Cache this locally to avoid having to look it up often:
      this.globalMarshaller = gcr.getComponent(StreamingMarshaller.class);
   }

   @Override
   public Set<Class<? extends CacheRpcCommand>> getTypeClasses() {
      //noinspection unchecked
      Set<Class<? extends CacheRpcCommand>> coreCommands = Util.asSet(DistributedExecuteCommand.class,
               LockControlCommand.class,
               StateRequestCommand.class, StateResponseCommand.class, ClusteredGetCommand.class,
               SingleRpcCommand.class, CommitCommand.class,
               PrepareCommand.class, RollbackCommand.class, RemoveCacheCommand.class,
               TxCompletionNotificationCommand.class, GetInDoubtTransactionsCommand.class,
               GetInDoubtTxInfoCommand.class, CompleteTransactionCommand.class,
               VersionedPrepareCommand.class, CreateCacheCommand.class, CancelCommand.class,
               VersionedCommitCommand.class, XSiteAdminCommand.class, TotalOrderNonVersionedPrepareCommand.class,
               TotalOrderVersionedPrepareCommand.class, TotalOrderCommitCommand.class,
               TotalOrderVersionedCommitCommand.class, TotalOrderRollbackCommand.class,
               XSiteStateTransferControlCommand.class, XSiteStatePushCommand.class, SingleXSiteRpcCommand.class,
               ClusteredGetAllCommand.class,
               StreamRequestCommand.class, StreamSegmentResponseCommand.class, StreamResponseCommand.class);
      // Only interested in cache specific replicable commands
      coreCommands.addAll(gcr.getModuleProperties().moduleCacheRpcCommands());
      return coreCommands;
   }

   @Override
   public void writeObject(ObjectOutput output, CacheRpcCommand command) throws IOException {
      //header: type + method id.
      cmdExt.writeCommandHeader(output, command);
      ByteString cacheName = command.getCacheName();
      ByteString.writeObject(output, cacheName);

      // Take the cache marshaller and generate the payload for the rest of
      // the command using that cache marshaller and the write the bytes in
      // the original payload.
      marshallParameters(command, globalMarshaller, output);
   }

   private void marshallParameters(CacheRpcCommand cmd, StreamingMarshaller marshaller, ObjectOutput oo) throws IOException {
      cmdExt.writeCommandParameters(oo, cmd);
   }

   @Override
   public CacheRpcCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      //header
      byte type = input.readByte();
      byte methodId = (byte) input.readShort();
      ByteString cacheName = ByteString.readObject(input);

      //create the object input
      CacheRpcCommand cacheRpcCommand = cmdExt.fromStream(methodId, type, cacheName);
      cmdExt.readCommandParameters(input, cacheRpcCommand);
      return cacheRpcCommand;
   }

   @Override
   public Integer getId() {
      return Ids.CACHE_RPC_COMMAND;
   }

   private static OutputStream convertObjectOutput(ObjectOutput objectOutput) {
      return objectOutput instanceof OutputStream ?
            (OutputStream) objectOutput :
            new DelegatingObjectOutput(objectOutput);
   }

}
