package org.infinispan.commons.io;

import static org.infinispan.commons.io.UnsignedNumeric.readUnsignedInt;
import static org.infinispan.commons.io.UnsignedNumeric.writeUnsignedInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * Variable length encoding for signed numbers, using the ZigZag technique
 * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">
 *    https://developers.google.com/protocol-buffers/docs/encoding#types</a>
 *
 * @author gustavonalle
 * @since 8.0
 */
public final class SignedNumeric {

   private SignedNumeric() {
   }

   public static int readSignedInt(ObjectInput in) throws IOException {
      return decode(readUnsignedInt(in));
   }

   public static int readSignedInt(InputStream in) throws IOException {
      return decode(readUnsignedInt(in));
   }

   public static void writeSignedInt(ObjectOutput out, int i) throws IOException {
      writeUnsignedInt(out, encode(i));
   }

   public static void writeSignedInt(OutputStream out, int i) throws IOException {
      writeUnsignedInt(out, encode(i));
   }

   public static int decode(int vint) {
      return (vint & 1) == 0 ? vint >>> 1 : ~(vint >>> 1);
   }

   public static int encode(int vint) {
      return (vint << 1) ^ (vint >> 31);
   }

}
