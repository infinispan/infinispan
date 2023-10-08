package org.infinispan.remoting.responses;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.impl.WriteSkewHelper;

/**
 * A {@link ValidResponse} used by Optimistic Transactions.
 * <p>
 * It contains the new {@link IncrementableEntryVersion} for each key updated.
 * <p>
 * To be extended in the future.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.PREPARE_RESPONSE)
public class PrepareResponse implements ValidResponse<Void> {

   private Map<Object, IncrementableEntryVersion> newWriteSkewVersions;
   private Map<Integer, IracMetadata> newIracMetadata;

   public static PrepareResponse asPrepareResponse(Object rv) {
      assert rv == null || rv instanceof PrepareResponse;
      return rv == null ? new PrepareResponse() : (PrepareResponse) rv;
   }

   public PrepareResponse() {
   }

   @ProtoFactory
   PrepareResponse(MarshallableMap<Object, IncrementableEntryVersion> newWriteSkewVersions,
                   Map<Integer, IracMetadata> newIracMetadata) {
      this.newWriteSkewVersions = MarshallableMap.unwrap(newWriteSkewVersions);
      this.newIracMetadata = newIracMetadata;
   }

   @ProtoField(1)
   MarshallableMap<Object, IncrementableEntryVersion> getNewWriteSkewVersions() {
      return MarshallableMap.create(newWriteSkewVersions);
   }

   @ProtoField(2)
   Map<Integer, IracMetadata> getNewIracMetadata() {
      return newIracMetadata;
   }

   @Override
   public Void getResponseValue() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      return "PrepareResponse{" +
             "WriteSkewVersions=" + newWriteSkewVersions +
             ", IracMetadataMap=" + newIracMetadata +
             '}';
   }

   public IracMetadata getIracMetadata(int segment) {
      return newIracMetadata != null ? newIracMetadata.get(segment) : null;
   }

   public void setNewIracMetadata(Map<Integer, IracMetadata> map) {
      this.newIracMetadata = map;
   }

   public void merge(PrepareResponse remote) {
      if (remote.newWriteSkewVersions != null) {
         mergeEntryVersions(remote.newWriteSkewVersions);
      }
      if (remote.newIracMetadata != null) {
         if (newIracMetadata == null) {
            newIracMetadata = new HashMap<>(remote.newIracMetadata);
         } else {
            newIracMetadata.putAll(remote.newIracMetadata);
         }
      }
   }

   public Map<Object, IncrementableEntryVersion> mergeEntryVersions(Map<Object, IncrementableEntryVersion> entryVersions) {
      if (newWriteSkewVersions == null) {
         newWriteSkewVersions = new HashMap<>();
      }
      newWriteSkewVersions = WriteSkewHelper.mergeEntryVersions(newWriteSkewVersions, entryVersions);
      return newWriteSkewVersions;
   }
}
