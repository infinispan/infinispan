package org.infinispan.xsite.spi;

import java.util.Objects;

import org.infinispan.metadata.Metadata;

/**
 * It stores the entry value and {@link Metadata} for a specific site.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class SiteEntry<V> {

   private final String siteName;
   private final V value;
   private final Metadata metadata;

   public SiteEntry(String siteName, V value, Metadata metadata) {
      this.siteName = Objects.requireNonNull(siteName, "Site Name must be non-null");
      this.value = value;
      this.metadata = metadata;
   }

   public String getSiteName() {
      return siteName;
   }

   public V getValue() {
      return value;
   }

   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      SiteEntry<?> siteEntry = (SiteEntry<?>) o;
      return siteName.equals(siteEntry.siteName) &&
            Objects.equals(value, siteEntry.value) &&
            Objects.equals(metadata, siteEntry.metadata);
   }

   @Override
   public int hashCode() {
      return Objects.hash(siteName, value, metadata);
   }

   @Override
   public String toString() {
      return "SiteEntry{" +
            "siteName='" + siteName + '\'' +
            ", value=" + value +
            ", metadata=" + metadata +
            '}';
   }
}
