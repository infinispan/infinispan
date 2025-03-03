package org.infinispan.commands.irac;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * A request for a new {@link IracMetadata} for a giver segment.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_METADATA_REQUEST_COMMAND)
public class IracMetadataRequestCommand extends BaseIracCommand implements TopologyAffectedCommand {

   @ProtoField(2)
   int segment;
   @ProtoField(3)
   int topologyId;
   @ProtoField(4)
   final IracEntryVersion versionSeen;

   public IracMetadataRequestCommand(ByteString cacheName, int segment, IracEntryVersion versionSeen) {
      this(cacheName, segment, -1, versionSeen);
   }

   @ProtoFactory
   IracMetadataRequestCommand(ByteString cacheName, int segment, int topologyId, IracEntryVersion versionSeen) {
      super(cacheName);
      this.segment = segment;
      this.topologyId = topologyId;
      this.versionSeen = versionSeen;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      return completedFuture(registry.getIracVersionGenerator().running().generateNewMetadata(segment, versionSeen));
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
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
