package org.infinispan.interceptors.distribution;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.VERSIONED_RESULTS)
public class VersionedResults {
   public final Object[] values;
   public final EntryVersion[] versions;

   public VersionedResults(Object[] values, EntryVersion[] versions) {
      this.values = values;
      this.versions = versions;
   }

   @ProtoFactory
   VersionedResults(MarshallableArray<Object> values, MarshallableArray<EntryVersion> versions) {
      this.values = MarshallableArray.unwrap(values, new Object[0]);
      this.versions = MarshallableArray.unwrap(versions, new EntryVersion[0]);
   }

   @ProtoField(number = 1)
   MarshallableArray<Object> getValues() {
      return MarshallableArray.create(values);
   }

   // We have to marshall as MarshallableArray, instead of two fields for SimpleClusteredVersion and NumericVersion
   // as index of version corresponds to values collection.
   @ProtoField(number = 2)
   MarshallableArray<EntryVersion> getVersions() {
      return MarshallableArray.create(versions);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("VersionedResults{");
      for (int i = 0; i < values.length; ++i) {
         sb.append(values[i]).append(" (").append(versions[i]).append(')');
         if (i != values.length - 1) sb.append(", ");
      }
      sb.append('}');
      return sb.toString();
   }
}
