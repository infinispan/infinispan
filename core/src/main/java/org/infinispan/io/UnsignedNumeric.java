package org.infinispan.io;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Helper to read and write unsigned numerics
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnsignedNumeric {
   /**
    * Reads an int stored in variable-length format.  Reads between one and five bytes.  Smaller values take fewer
    * bytes.  Negative numbers are not supported.
    */
   public static int readUnsignedInt(ObjectInput in) throws IOException {
      byte b = in.readByte();
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Writes an int in a variable-length format.  Writes between one and five bytes.  Smaller values take fewer bytes.
    * Negative numbers are not supported.
    *
    * @param i int to write
    */
   public static void writeUnsignedInt(ObjectOutput out, int i) throws IOException {
      while ((i & ~0x7F) != 0) {
         out.writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      out.writeByte((byte) i);
   }


   /**
    * Reads an int stored in variable-length format.  Reads between one and nine bytes.  Smaller values take fewer
    * bytes.  Negative numbers are not supported.
    */
   public static long readUnsignedLong(ObjectInput in) throws IOException {
      byte b = in.readByte();
      long i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Writes an int in a variable-length format.  Writes between one and nine bytes.  Smaller values take fewer bytes.
    * Negative numbers are not supported.
    *
    * @param i int to write
    */
   public static void writeUnsignedLong(ObjectOutput out, long i) throws IOException {
      while ((i & ~0x7F) != 0) {
         out.writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      out.writeByte((byte) i);
   }

}
