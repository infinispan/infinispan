package org.infinispan.commands.remote;

import java.io.IOException;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;
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
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeUserArray(keys);
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallArray(input, Object[]::new, MarshalledEntryUtil::readKey);
   }
}
