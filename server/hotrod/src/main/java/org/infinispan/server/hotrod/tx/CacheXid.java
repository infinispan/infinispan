package org.infinispan.server.hotrod.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.core.ExternalizerIds;
import org.infinispan.util.ByteString;

/**
 * A key used in the global transaction table.
 * <p>
 * The global transaction table is a replicated cache. This key contains the cache name and the transactions' {@link
 * XidImpl}.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class CacheXid {

   public static final AdvancedExternalizer<CacheXid> EXTERNALIZER = new Externalizer();

   private final ByteString cacheName;
   private final XidImpl xid;

   CacheXid(ByteString cacheName, XidImpl xid) {
      this.cacheName = cacheName;
      this.xid = xid;
   }

   public static void writeTo(ObjectOutput output, CacheXid object) throws IOException {
      ByteString.writeObject(output, object.cacheName);
      XidImpl.writeTo(output, object.xid);
   }

   public static CacheXid readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      return new CacheXid(ByteString.readObject(input), XidImpl.readFrom(input));
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      CacheXid cacheXid = (CacheXid) o;
      return cacheName.equals(cacheXid.cacheName) && xid.equals(cacheXid.xid);
   }

   @Override
   public int hashCode() {
      int result = cacheName.hashCode();
      result = 31 * result + xid.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "CacheXid{" +
            "cacheName=" + cacheName +
            ", xid=" + xid +
            '}';
   }

   private static class Externalizer implements AdvancedExternalizer<CacheXid> {

      @Override
      public Set<Class<? extends CacheXid>> getTypeClasses() {
         return Collections.singleton(CacheXid.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CACHE_XID;
      }

      @Override
      public void writeObject(ObjectOutput output, CacheXid object) throws IOException {
         writeTo(output, object);
      }

      @Override
      public CacheXid readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return readFrom(input);
      }
   }
}
