package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.scattered.BiasManager;
import org.infinispan.util.ByteString;

public class RenewBiasCommand extends BaseRpcCommand {
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

   public void init(BiasManager biasManager) {
      this.biasManager = biasManager;
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
   public void writeTo(ObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      MarshalledEntryUtil.marshallArray(keys, (key, factory, out) -> MarshalledEntryUtil.writeKey(key));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallArray(input, Object[]::new, MarshalledEntryUtil::readKey);
   }
}
