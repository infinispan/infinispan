package org.infinispan.marshall.core.internal;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AdvancedExternalizer;

import java.io.ObjectOutput;

/**
 *
 * @param <O> Output type where data is encoded into,
 * could be {@link ObjectOutput}, {@link java.nio.ByteBuffer}
 * or similar abstractions.
 */
public interface Encoding<O, I> {

   void encodeBoolean(boolean b, O out);
   void encodeByte(int b, O out);
   void encodeBytes(byte[] b, int off, int len, O out);
   void encodeChar(int v, O out);
   void encodeDouble(double v, O out);
   void encodeFloat(float v, O out);
   void encodeInt(int v, O out);
   void encodeLong(long v, O out);
   void encodeShort(int v, O out);
   void encodeString(String s, O out);
   void encodeStringUtf8(String s, O out);

   boolean decodeBoolean(I in);
   byte decodeByte(I in);
   void decodeBytes(byte[] b, int off, int len, I in);
   char decodeChar(I in);
   double decodeDouble(I in);
   float decodeFloat(I in);
   int decodeInt(I in);
   long decodeLong(I in);
   short decodeShort(I in);
   String decodeString(I in);
   String decodeStringUtf8(I in);
   int decodeUnsignedShort(I in);

//   // TODO: Returns Infinispan's ByteBuffer for ease of use, but this should change
//   // e.g. NIO ByteBuffer? Another abstraction not exposing byte[]?
//   ByteBuffer toByteBuffer(Object obj, int estimatedSize, AdvancedExternalizer ext);

}
