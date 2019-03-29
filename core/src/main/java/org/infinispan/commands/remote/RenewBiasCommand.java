package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.scattered.BiasManager;
import org.infinispan.util.ByteString;

public class RenewBiasCommand extends BaseRpcCommand implements InitializableCommand {
   public static final byte COMMAND_ID = 75;

   Object[] keys;
   transient BiasManager biasManager;

   public RenewBiasCommand() {
      super(null);
   }

   public RenewBiasCommand(ByteString cacheName) {
      super(cacheName);
   }

   public RenewBiasCommand(ByteString cacheName, Object[] keys) {
      super(cacheName);
      this.keys = keys;
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.biasManager = componentRegistry.getBiasManager().running();
   }

   @Override
   public Object invoke() throws Throwable {
      for (Object key : keys) {
         biasManager.renewRemoteBias(key, getOrigin());
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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallArray(keys, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallArray(input, Object[]::new);
   }
}
