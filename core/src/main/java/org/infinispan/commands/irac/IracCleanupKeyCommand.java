package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.irac.IracManager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class IracCleanupKeyCommand implements CacheRpcCommand, InitializableCommand {

   public static final byte COMMAND_ID = 84;

   private ByteString cacheName;
   private Object key;
   private Object lockOwner;

   private IracManager iracManager;

   @SuppressWarnings("unused")
   public IracCleanupKeyCommand() {
   }

   public IracCleanupKeyCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracCleanupKeyCommand(ByteString cacheName, Object key, Object lockOwner) {
      this.cacheName = cacheName;
      this.key = key;
      this.lockOwner = lockOwner;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      iracManager.cleanupKey(key, lockOwner);
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
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      boolean cId = lockOwner instanceof CommandInvocationId;
      if (cId) {
         CommandInvocationId.writeTo(output, (CommandInvocationId) lockOwner);
      } else {
         output.writeObject(lockOwner);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
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
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.iracManager = componentRegistry.getIracManager().running();
   }
}
