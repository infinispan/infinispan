package org.infinispan.query.remote.impl;

import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.dataconversion.WrapperIds;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;

/**
 * Wraps byte[] in a {@link ProtobufValueWrapper} in order to make the payload indexable by Hibernate Search.
 *
 * @since 9.1
 */
public class ProtostreamWrapper implements Wrapper {

   public static final ProtostreamWrapper INSTANCE = new ProtostreamWrapper();

   private ProtostreamWrapper() {
   }

   @Override
   public Object wrap(Object value) {
      if (value instanceof byte[]) {
         return new ProtobufValueWrapper((byte[]) value);
      }
      if (value instanceof WrappedByteArray) {
         return new ProtobufValueWrapper(((WrappedByteArray) value).getBytes());
      }
      return value;
   }

   @Override
   public Object unwrap(Object target) {
      if (target instanceof ProtobufValueWrapper) {
         return ((ProtobufValueWrapper) target).getBinary();
      }
      if(target instanceof WrappedByteArray) {
         return WrappedByteArray.class.cast(target).getBytes();
      }
      return target;
   }

   @Override
   public byte id() {
      return WrapperIds.PROTOSTREAM_WRAPPER;
   }

   @Override
   public boolean isFilterable() {
      return true;
   }

}
