package org.infinispan.interceptors.distribution;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.VERSIONED_RESULT)
public class VersionedResult {
   public final Object result;
   public final EntryVersion version;

   public VersionedResult(Object result, EntryVersion version) {
      this.result = result;
      this.version = version;
   }

   @ProtoFactory
   VersionedResult(MarshallableObject<?> result, NumericVersion numericVersion, SimpleClusteredVersion clusteredVersion) {
      this.result = MarshallableObject.unwrap(result);
      this.version = numericVersion != null ? numericVersion : clusteredVersion;
   }

   @ProtoField(1)
   MarshallableObject<?> getResult() {
      return MarshallableObject.create(result);
   }

   @ProtoField(2)
   NumericVersion getNumericVersion() {
      return version instanceof NumericVersion ? (NumericVersion) version : null;
   }

   @ProtoField(3)
   SimpleClusteredVersion getClusteredVersion() {
      return version instanceof SimpleClusteredVersion ? (SimpleClusteredVersion) version : null;
   }

   @Override
   public String toString() {
      return "VersionedResult{" + result + " (" + version + ")}";
   }
}
