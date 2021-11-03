package org.infinispan.metadata.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The metadata stored for an entry needed for IRAC (async cross-site replication).
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_METADATA)
public class IracMetadata {

   private final String site;
   private final IracEntryVersion version;

   @ProtoFactory
   public IracMetadata(String site, IracEntryVersion version) {
      this.site = Objects.requireNonNull(site);
      this.version = Objects.requireNonNull(version);
   }

   public static void writeTo(ObjectOutput output, IracMetadata metadata) throws IOException {
      if (metadata == null) {
         output.writeObject(null);
         return;
      }
      output.writeObject(metadata.version);
      output.writeUTF(metadata.site);
   }

   public static IracMetadata readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
      IracEntryVersion version = (IracEntryVersion) in.readObject();
      return version == null ? null : new IracMetadata(in.readUTF(), version);
   }

   @ProtoField(1)
   public String getSite() {
      return site;
   }

   @ProtoField(2)
   public IracEntryVersion getVersion() {
      return version;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      IracMetadata that = (IracMetadata) o;

      return site.equals(that.site) && version.equals(that.version);
   }

   @Override
   public int hashCode() {
      int result = site.hashCode();
      result = 31 * result + version.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "IracMetadata{" +
            "site='" + site + '\'' +
            ", version=" + version +
            '}';
   }
}
