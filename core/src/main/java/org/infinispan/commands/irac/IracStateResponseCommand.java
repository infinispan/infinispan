package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * The IRAC state for a given key.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracStateResponseCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 120;

   private ByteString cacheName;
   private int segment;
   private Object key;
   private Object lockOwner;
   private IracMetadata tombstone;

   @SuppressWarnings("unused")
   public IracStateResponseCommand() {
   }

   public IracStateResponseCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracStateResponseCommand(ByteString cacheName, int segment, Object key, Object lockOwner, IracMetadata tombstone) {
      this(cacheName);
      this.segment = segment;
      this.key = key;
      this.lockOwner = lockOwner;
      this.tombstone = tombstone;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      registry.getIracManager().wired().receiveState(segment, key, lockOwner, tombstone);
      return CompletableFutures.completedNull();
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
      output.writeInt(segment);
      output.writeObject(key);
      boolean cId = lockOwner instanceof CommandInvocationId;
      output.writeBoolean(cId);
      if (cId) {
         CommandInvocationId.writeTo(output, (CommandInvocationId) lockOwner);
      } else {
         output.writeObject(lockOwner);
      }
      boolean nullTombstone = tombstone == null;
      output.writeBoolean(nullTombstone);
      if (!nullTombstone) {
         tombstone.writeTo(output);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.segment = input.readInt();
      this.key = input.readObject();
      if (input.readBoolean()) {
         lockOwner = CommandInvocationId.readFrom(input);
      } else {
         this.lockOwner = input.readObject();
      }
      if (input.readBoolean()) {
         tombstone = null;
      } else {
         tombstone = IracMetadata.readFrom(input);
      }
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public Address getOrigin() {
      //no-op
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }

   @Override
   public String toString() {
      return "IracStateResponseCommand{" +
            "cacheName=" + cacheName +
            ", key=" + key +
            ", lockOwner=" + lockOwner +
            ", tombstone=" + tombstone +
            '}';
   }
}
