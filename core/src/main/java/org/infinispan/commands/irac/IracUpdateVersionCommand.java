package org.infinispan.commands.irac;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * It transfers the current versions stored in {@link IracVersionGenerator} to the other nodes when joins/leaving events
 * occurs.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class IracUpdateVersionCommand implements CacheRpcCommand {
   public static final byte COMMAND_ID = 32;
   private final ByteString cacheName;
   private Map<Integer, IracEntryVersion> versions;

   public IracUpdateVersionCommand() {
      this(null);
   }

   public IracUpdateVersionCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracUpdateVersionCommand(ByteString cacheName, Map<Integer, IracEntryVersion> segmentsVersion) {
      this(cacheName);
      this.versions = segmentsVersion;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      IracVersionGenerator versionGenerator = registry.getIracVersionGenerator().running();
      versions.forEach(versionGenerator::updateVersion);
      return CompletableFutures.completedNull();
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
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
   public Address getOrigin() {
      //no-op, not required.
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op, not required.
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallMap(versions, DataOutput::writeInt, ObjectOutput::writeObject, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      versions = MarshallUtil.unmarshallMap(input, DataInput::readInt, IracUpdateVersionCommand::read, HashMap::new);
   }

   private static IracEntryVersion read(ObjectInput input) throws IOException, ClassNotFoundException {
      return (IracEntryVersion) input.readObject();
   }

   @Override
   public String toString() {
      return "IracUpdateVersionCommand{" +
            "cacheName=" + cacheName +
            ", versions=" + versions +
            '}';
   }
}
