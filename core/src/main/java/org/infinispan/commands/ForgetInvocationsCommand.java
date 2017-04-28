package org.infinispan.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

public class ForgetInvocationsCommand implements CacheRpcCommand {
   public static final byte COMMAND_ID = 67;
   private final ByteString cacheName;
   private Address origin;
   // All the commands share the same origin
   private Object[] keys;
   private long[] ids;
   private DataContainer dataContainer;

   public ForgetInvocationsCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public ForgetInvocationsCommand() {
      this.cacheName = null;
   }

   public void init(DataContainer dataContainer) {
      this.dataContainer = dataContainer;
   }

   public void set(Object[] keys, long[] ids) {
      this.keys = keys;
      this.ids = ids;
   }

   public Object[] keys() {
      return keys;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public void setOrigin(Address origin) {
      this.origin = origin;
   }

   @Override
   public Address getOrigin() {
      return origin;
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
   public boolean canBlock() {
      return false;
   }

   @Override
   public Object invoke() {
      for (int i = 0; i < keys.length; ++i) {
         long id = ids[i];
         dataContainer.compute(keys[i], (key, oldEntry, factory) -> {
            if (oldEntry == null) {
               return null;
            }
            Metadata metadata = oldEntry.getMetadata();
            if (metadata == null) {
               return oldEntry;
            }
            InvocationRecord records = metadata.lastInvocation();
            InvocationRecord purged = InvocationRecord.purgeCompleted(records, commandId ->
               commandId.getAddress().equals(getOrigin()) && id == commandId.getId());
            if (purged == null && oldEntry.getValue() == null) {
               return null;
            } else {
               return factory.update(oldEntry, metadata.builder().invocations(purged).build(), true);
            }
         });
      }
      return null;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallSize(output, keys.length);
      assert keys.length == ids.length;
      for (Object key : keys) output.writeObject(key);
      for (long id : ids) output.writeLong(id);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      int size = MarshallUtil.unmarshallSize(input);
      keys = new Object[size];
      ids = new long[size];
      for (int i = 0; i < size; ++i) keys[i] = input.readObject();
      for (int i = 0; i < size; ++i) ids[i] = input.readLong();
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("ForgetInvocationsCommand{origin=").append(origin).append(", invalidations=");
      if (keys.length > 0) {
         sb.append(ids[0]).append(": ").append(keys[0]);
      }
      for (int i = 1; i < keys.length; ++i) {
         sb.append(", ").append(ids[i]).append(": ").append(keys[i]);
      }
      return sb.append('}').toString();
   }
}
