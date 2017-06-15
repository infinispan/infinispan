package org.infinispan.commons.dataconversion;

import org.infinispan.commons.marshall.WrappedByteArray;

/**
 * Wraps byte[] on a {@link WrappedByteArray} to provide equality and hashCode support, leaving other objects
 * unchanged.
 *
 * @since 9.1
 */
public class ByteArrayWrapper implements Wrapper {

   public static final ByteArrayWrapper INSTANCE = new ByteArrayWrapper();

   @Override
   public Object wrap(Object obj) {
      if (obj instanceof byte[]) return new WrappedByteArray((byte[]) obj);
      return obj;
   }

   @Override
   public Object unwrap(Object obj) {
      if (obj instanceof WrappedByteArray) return WrappedByteArray.class.cast(obj).getBytes();
      return obj;
   }

}
