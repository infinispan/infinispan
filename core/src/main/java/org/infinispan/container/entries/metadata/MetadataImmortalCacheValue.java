package org.infinispan.container.entries.metadata;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * A form of {@link org.infinispan.container.entries.ImmortalCacheValue} that
 * is {@link org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataImmortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;

   public MetadataImmortalCacheValue(Object value, Metadata metadata) {
      super(value);
      this.metadata = metadata;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new MetadataImmortalCacheEntry(key, value, metadata);
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
   public String toString() {
      return getClass().getSimpleName() + " {" +
            "value=" + toStr(value) +
            ", metadata=" + metadata +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<MetadataImmortalCacheValue> {
      @Override
      public void writeObject(UserObjectOutput output, MetadataImmortalCacheValue icv) throws IOException {
         output.writeObject(icv.value);
         output.writeObject(icv.metadata);
      }

      @Override
      public MetadataImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new MetadataImmortalCacheValue(v, metadata);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_IMMORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataImmortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends MetadataImmortalCacheValue>>asSet(MetadataImmortalCacheValue.class);
      }
   }

}
