package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.expiration.impl.ClusterExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;

public class IracTouchKeyCommand extends ForwardableCommand<Boolean> {
   public static final byte COMMAND_ID = 29;

   private Object key;

   @SuppressWarnings("unused")
   public IracTouchKeyCommand() {
      super(COMMAND_ID, null);
   }

   public IracTouchKeyCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public IracTouchKeyCommand(ByteString cacheName, Object key) {
      super(COMMAND_ID, cacheName);
      this.key = key;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      return null;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
   }

   @Override
   public CompletionStage<Boolean> executeOperation(BackupReceiver receiver) {
      return receiver.touchEntry(key);
   }

   @Override
   public ResponseCollector<Boolean> responseCollector() {
      return ClusterExpirationManager.TouchResponseCollector.INSTANCE;
   }

   @Override
   public CompletionStage<Boolean> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      assert !preserveOrder : "IRAC Touch Command sent asynchronously!";
      return receiver.touchEntry(key);
   }
}
