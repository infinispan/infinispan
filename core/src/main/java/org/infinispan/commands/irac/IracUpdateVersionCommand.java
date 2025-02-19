package org.infinispan.commands.irac;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * It transfers the current versions stored in {@link IracVersionGenerator} to the other nodes when joins/leaving events
 * occurs.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_UPDATE_VERSION_COMMAND)
public class IracUpdateVersionCommand extends BaseIracCommand {
   public static final byte COMMAND_ID = 32;

   @ProtoField(2)
   final Map<Integer, IracEntryVersion> versions;

   @ProtoFactory
   public IracUpdateVersionCommand(ByteString cacheName, Map<Integer, IracEntryVersion> versions) {
      super(cacheName);
      this.versions = versions;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      IracVersionGenerator versionGenerator = registry.getIracVersionGenerator().running();
      versions.forEach(versionGenerator::updateVersion);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "IracUpdateVersionCommand{" +
            "cacheName=" + cacheName +
            ", versions=" + versions +
            '}';
   }
}
