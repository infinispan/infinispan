package org.infinispan.container.entries.metadata;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static org.infinispan.commons.util.Util.toStr;

/**
 * A form of {@link org.infinispan.container.entries.ImmortalCacheEntry} that
 * is {@link org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataImmortalCacheEntry extends ImmortalCacheEntry implements MetadataAware {

   protected Metadata metadata;

   public MetadataImmortalCacheEntry(Object key, Object value, Metadata metadata) {
      super(key, value);
      this.metadata = metadata;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new MetadataImmortalCacheValue(value, metadata);
   }

   @Override
   public String toString() {
      return String.format("MetadataImmortalCacheEntry{key=%s, value=%s, metadata=%s}",
            toStr(key), toStr(value), metadata);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataImmortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataImmortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.value);
         output.writeObject(ice.metadata);
      }

      @Override
      public MetadataImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new MetadataImmortalCacheEntry(k, v, metadata);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_IMMORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataImmortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends MetadataImmortalCacheEntry>>asSet(MetadataImmortalCacheEntry.class);
      }
   }
}
