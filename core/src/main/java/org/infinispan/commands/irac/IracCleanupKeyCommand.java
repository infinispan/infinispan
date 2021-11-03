package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Sends a cleanup request from primary owner to backup owners.
 * <p>
 * Sent after a successful update of all remote sites.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracCleanupKeyCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 122;

   private ByteString cacheName;
   private int segment;
   private Object key;
   private Object lockOwner;

   @SuppressWarnings("unused")
   public IracCleanupKeyCommand() {
   }

   public IracCleanupKeyCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracCleanupKeyCommand(ByteString cacheName, int segment, Object key, Object lockOwner) {
      this.cacheName = cacheName;
      this.segment = segment;
      this.key = key;
      this.lockOwner = lockOwner;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletableFuture<Object> invokeAsync(ComponentRegistry componentRegistry) {
      componentRegistry.getIracManager().running().cleanupKey(segment, key, lockOwner);
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
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.segment = input.readInt();
      this.key = input.readObject();
      boolean cId = input.readBoolean();
      if (cId) {
         lockOwner = CommandInvocationId.readFrom(input);
      } else {
         this.lockOwner = input.readObject();
      }
   }

   @Override
   public Address getOrigin() {
      //not needed
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }

   @Override
   public String toString() {
      return "IracCleanupKeyCommand{" +
            "cacheName=" + cacheName +
            ", key=" + Util.toStr(key) +
            ", lockOwner=" + lockOwner +
            '}';
   }
}
