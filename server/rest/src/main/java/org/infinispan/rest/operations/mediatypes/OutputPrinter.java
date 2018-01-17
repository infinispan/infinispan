package org.infinispan.rest.operations.mediatypes;

import java.io.UnsupportedEncodingException;

import org.infinispan.CacheSet;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.rest.operations.exceptions.ServerInternalException;

/**
 * Converts binary array from {@link org.infinispan.Cache} into output format.
 *
 * <p>
 *    In order to avoid unnecessary conversion steps, all methods need to return a byte array. This way
 *    Netty doesn't need to do any conversion - it just wraps it into a {@link io.netty.buffer.ByteBuf}.
 * </p>
 *
 * @author Sebastian Łaskawiec
 */
public interface OutputPrinter {

   /**
    * Converts all values in the cache to a desired output format.
    *
    * @param cacheName Cache name (sometimes might be used as xml or json key).
    * @param cacheSet Key Set.
    * @param charset Desired {@link Charset}
    * @return Byte array representation of converted values.
    * @throws ServerInternalException Thrown if conversion was not successful.
    */
   byte[] print(String cacheName, CacheSet<?> cacheSet, Charset charset) throws ServerInternalException;

   default String asString(Object k) {
      try {
         if (k instanceof byte[]) {
            return new String((byte[]) k, "UTF-8");
         }
      } catch (UnsupportedEncodingException e) {
         throw new EncodingException("Cannot convert key to string", e);
      }
      return k.toString();
   }
}
