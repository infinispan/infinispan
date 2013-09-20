package org.infinispan.metadata;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static java.lang.Math.min;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class InternalMetadataImpl implements InternalMetadata {
   private final Metadata actual;
   private final long created;
   private final long lastUsed;

   public InternalMetadataImpl() {
      //required by the AZUL VM in order to run the tests
      this(null,-1,-1);
   }

   public InternalMetadataImpl(InternalCacheEntry ice) {
      this(ice.getMetadata(), ice.getCreated(), ice.getLastUsed());
   }

   public InternalMetadataImpl(Metadata actual, long created, long lastUsed) {
      this.actual = actual;
      this.created = created;
      this.lastUsed = lastUsed;
   }

   @Override
   public long lifespan() {
      return actual.lifespan();
   }

   @Override
   public long maxIdle() {
      return actual.maxIdle();
   }

   @Override
   public EntryVersion version() {
      return actual.version();
   }

   @Override
   public Builder builder() {
      return actual.builder();
   }

   @Override
   public long created() {
      return created;
   }

   @Override
   public long lastUsed() {
      return lastUsed;
   }

   public Metadata actual() {
      return actual;
   }

   @Override
   public long expiryTime() {
      long lifespan = actual.lifespan();
      long lset = lifespan > -1 ? created + lifespan : -1;
      long maxIdle = actual.maxIdle();
      long muet = maxIdle > -1 ? lastUsed + maxIdle : -1;
      if (lset == -1) return muet;
      if (muet == -1) return lset;
      return min(lset, muet);
   }

   @Override
   public boolean isExpired(long now) {
      long expiry = expiryTime();
      return expiry > 0 && expiry <= now;
   }

   public Metadata getActual() {
      return actual;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof InternalMetadataImpl)) return false;

      InternalMetadataImpl that = (InternalMetadataImpl) o;

      if (created != that.created) return false;
      if (lastUsed != that.lastUsed) return false;
      if (actual != null ? !actual.equals(that.actual) : that.actual != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = actual != null ? actual.hashCode() : 0;
      result = 31 * result + (int) (created ^ (created >>> 32));
      result = 31 * result + (int) (lastUsed ^ (lastUsed >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "InternalMetadataImpl{" +
            "actual=" + actual +
            ", created=" + created +
            ", lastUsed=" + lastUsed +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<InternalMetadataImpl> {

      private static final long serialVersionUID = -5291318076267612501L;

      public Externalizer() {
      }

      @Override
      public void writeObject(ObjectOutput output, InternalMetadataImpl b) throws IOException {
         output.writeLong(b.created);
         output.writeLong(b.lastUsed);
         output.writeObject(b.actual);
      }

      @Override
      public InternalMetadataImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         long created = input.readLong();
         long lastUsed= input.readLong();
         Metadata actual = (Metadata) input.readObject();
         return new InternalMetadataImpl(actual, created, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.INTERNAL_METADATA_ID;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends InternalMetadataImpl>> getTypeClasses() {
         return Util.<Class<? extends InternalMetadataImpl>>asSet(InternalMetadataImpl.class);
      }
   }
}
