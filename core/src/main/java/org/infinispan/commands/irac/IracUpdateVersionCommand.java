package org.infinispan.commands.irac;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

/**
 * It transfers the current versions stored in {@link IracVersionGenerator} to the other nodes when joins/leaving events
 * occurs.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_UPDATE_VERSION_COMMAND)
public class IracUpdateVersionCommand extends BaseRpcCommand {
   public static final byte COMMAND_ID = 32;
   final Map<Integer, IracEntryVersion> versions;

   public IracUpdateVersionCommand(ByteString cacheName, Map<Integer, IracEntryVersion> segmentsVersion) {
      super(cacheName);
      this.versions = segmentsVersion;
   }

   @ProtoFactory
   IracUpdateVersionCommand(ByteString cacheName, MarshallableMap<Integer, IracEntryVersion> wrappedSegmentVersions) {
      this(cacheName, MarshallableMap.unwrap(wrappedSegmentVersions));
   }

   @ProtoField(number = 2)
   MarshallableMap<Integer, IracEntryVersion> getWrappedSegmentVersions() {
      return MarshallableMap.create(versions);
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
   public String toString() {
      return "IracUpdateVersionCommand{" +
            "cacheName=" + cacheName +
            ", versions=" + versions +
            '}';
   }
}
