package org.infinispan.hotrod.impl.cache;

import java.util.Objects;

import org.infinispan.api.common.CacheEntryVersion;

/**
 * @since 14.0
 **/
public class CacheEntryVersionImpl implements CacheEntryVersion {
   private final long version;

   public CacheEntryVersionImpl(long version) {
      this.version = version;
   }

   public long version() {
      return version;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheEntryVersionImpl that = (CacheEntryVersionImpl) o;
      return version == that.version;
   }

   @Override
   public int hashCode() {
      return Objects.hash(version);
   }

   @Override
   public String toString() {
      return "CacheEntryVersionImpl{" +
            "version=" + version +
            '}';
   }
}
