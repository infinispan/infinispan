package org.infinispan.metadata.impl;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.XSiteNamedCache;

/**
 * The metadata stored for an entry needed for IRAC (async cross-site replication).
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_METADATA)
public class IracMetadata {

   private final ByteString site;
   private final IracEntryVersion version;

   @ProtoFactory
   public IracMetadata(String site, IracEntryVersion version) {
      this(XSiteNamedCache.cachedByteString(Objects.requireNonNull(site)), Objects.requireNonNull(version));
   }

   public IracMetadata(ByteString site, IracEntryVersion version) {
      this.site = Objects.requireNonNull(site);
      this.version = Objects.requireNonNull(version);
   }

   @ProtoField(1)
   public String getSite() {
      return site.toString();
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
