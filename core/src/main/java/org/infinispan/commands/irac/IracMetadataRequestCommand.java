package org.infinispan.commands.irac;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

/**
 * A request for a new {@link IracMetadata} for a giver segment.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracMetadataRequestCommand implements CacheRpcCommand, TopologyAffectedCommand {

   public static final byte COMMAND_ID = 124;

   private ByteString cacheName;
   private int segment;
   private int topologyId = -1;
   private IracEntryVersion versionSeen;

   public IracMetadataRequestCommand() {
   }

   public IracMetadataRequestCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracMetadataRequestCommand(ByteString cacheName, int segment, IracEntryVersion versionSeen) {
      this.cacheName = cacheName;
      this.segment = segment;
      this.versionSeen = versionSeen;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      return completedFuture(registry.getIracVersionGenerator().running().generateNewMetadata(segment, versionSeen));
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeInt(segment);
      output.writeObject(versionSeen);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.segment = input.readInt();
      this.versionSeen = (IracEntryVersion) input.readObject();
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
      return "IracMetadataRequestCommand{" +
            "cacheName=" + cacheName +
            ", segment=" + segment +
            ", topologyId=" + topologyId +
            ", versionSeen=" + versionSeen +
            '}';
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }
}
