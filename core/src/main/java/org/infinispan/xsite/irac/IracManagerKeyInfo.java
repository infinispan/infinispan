package org.infinispan.xsite.irac;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Pedro Ruivo
 * @since 14
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_MANAGER_KEY_INFO)
public class IracManagerKeyInfo {

   final int segment;
   final Object key;
   final Object owner;

   public IracManagerKeyInfo(int segment, Object key, Object owner) {
      this.segment = segment;
      this.key = Objects.requireNonNull(key);
      this.owner = Objects.requireNonNull(owner);
   }

   @ProtoFactory
   IracManagerKeyInfo(int segment, MarshallableObject<Object> wrappedKey, MarshallableObject<Object> wrappedOwner) {
      this(segment, MarshallableObject.unwrap(wrappedKey), MarshallableObject.unwrap(wrappedOwner));
   }

   public Object getKey() {
      return key;
   }

   public Object getOwner() {
      return owner;
   }

   @ProtoField(1)
   public int getSegment() {
      return segment;
   }

   @ProtoField(2)
   MarshallableObject<Object> getWrappedKey() {
      return MarshallableObject.create(key);
   }

   @ProtoField(3)
   MarshallableObject<Object> getWrappedOwner() {
      return MarshallableObject.create(owner);
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

   @Override
   public String toString() {
      return "IracManagerKeyInfoImpl{" + "segment=" + segment + ", key=" + key + ", owner=" + owner + '}';
   }
}
