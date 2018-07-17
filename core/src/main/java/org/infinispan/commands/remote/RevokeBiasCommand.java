package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.BiasManager;
import org.infinispan.util.ByteString;

/**
 * Informs node that it is not allowed to serve reads from the local record anymore.
 * After local bias is revoked a {@link BackupAckCommand} is sent to the originator, and this confirms all keys.
 */
//TODO: consolidate this with InvalidateVersionsCommand
public class RevokeBiasCommand extends BaseRpcCommand {
   public static final byte COMMAND_ID = 74;

   private Address ackTarget;
   private long id;
   private int topologyId;
   private Collection<Object> keys;
   private transient BiasManager biasManager;
   private transient CommandsFactory commandsFactory;
   private transient RpcManager rpcManager;

   public RevokeBiasCommand() {
      super(null);
   }

   public RevokeBiasCommand(ByteString cacheName) {
      super(cacheName);
   }

   public RevokeBiasCommand(ByteString cacheName, Address ackTarget, long id, int topologyId, Collection<Object> keys) {
      super(cacheName);
      this.ackTarget = ackTarget;
      this.id = id;
      this.topologyId = topologyId;
      this.keys = keys;
   }

   public void init(BiasManager biasManager, CommandsFactory commandsFactory, RpcManager rpcManager) {
      this.biasManager = biasManager;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
   }

   @Override
   public Object invoke() throws Throwable {
      for (Object key : keys) {
         biasManager.revokeLocalBias(key);
      }
      // ackTarget null means that this message is sent synchronously by primary owner == originator
      if (ackTarget != null) {
         BackupAckCommand backupAckCommand = commandsFactory.buildBackupAckCommand(id, topologyId);
         rpcManager.sendTo(ackTarget, backupAckCommand, DeliverOrder.NONE);
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeObject(ackTarget);
      if (ackTarget != null) {
         output.writeLong(id);
      }
      output.writeInt(topologyId);
      output.writeUserCollection(keys, UserObjectOutput::writeKey);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      ackTarget = (Address) input.readObject();
      if (ackTarget != null) {
         id = input.readLong();
      }
      topologyId = input.readInt();
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new, MarshalledEntryUtil::readKey);
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("RevokeBiasCommand{");
      sb.append("ackTarget=").append(ackTarget);
      sb.append(", id=").append(id);
      sb.append(", topologyId=").append(topologyId);
      sb.append(", keys=").append(keys);
      sb.append('}');
      return sb.toString();
   }
}
