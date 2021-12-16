package org.infinispan.xsite.irac;

import java.util.Objects;

/**
 * Default implementation of {@link IracManagerKeyInfo}.
 *
 * @author Pedro Ruivo
 * @since 14
 */
public class IracManagerKeyInfoImpl implements IracManagerKeyInfo {

   private final int segment;
   private final Object key;
   private final Object owner;

   public IracManagerKeyInfoImpl(int segment, Object key, Object owner) {
      this.segment = segment;
      this.key = Objects.requireNonNull(key);
      this.owner = Objects.requireNonNull(owner);
   }

   @Override
   public Object getKey() {
      return key;
   }

   @Override
   public Object getOwner() {
      return owner;
   }

   @Override
   public int getSegment() {
      return segment;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof IracManagerKeyInfo)) return false;

      IracManagerKeyInfo that = (IracManagerKeyInfo) o;

      if (segment != that.getSegment()) return false;
      if (!key.equals(that.getKey())) return false;
      return owner.equals(that.getOwner());
   }

   @Override
   public int hashCode() {
      int result = segment;
      result = 31 * result + key.hashCode();
      result = 31 * result + owner.hashCode();
      return result;
   }
}
