package org.infinispan.xsite.irac;

import java.util.Objects;

import org.infinispan.commands.RequestUUID;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
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
   final RequestUUID owner;

   public IracManagerKeyInfo(int segment, Object key, RequestUUID owner) {
      this.segment = segment;
      this.key = Objects.requireNonNull(key);
      this.owner = Objects.requireNonNull(owner);
   }

   @ProtoFactory
   IracManagerKeyInfo(int segment, MarshallableObject<Object> wrappedKey, RequestUUID owner) {
      this(segment, MarshallableObject.unwrap(wrappedKey), owner);
   }

   public Object getKey() {
      return key;
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
   public RequestUUID getOwner() {
      return owner;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof IracManagerKeyInfo that)) return false;

      return segment == that.getSegment() &&
            key.equals(that.getKey()) &&
            owner.equals(that.getOwner());
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
      return "IracManagerKeyInfoImpl{" + "segment=" + segment + ", key=" + Util.toStr(key) + ", owner=" + owner + '}';
   }
}
