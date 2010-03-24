package hotrod.impl.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class VHelper {
   private static final int MAX_VINT_BYTES = 5;
   private static final int MAX_VLONG_BYTES = 9;

   public static int readVInt(InputStream is) {
      int result = 0;
      for (int i = 0; i < MAX_VINT_BYTES; i++) {
         int aByte = nextByte(is);
         boolean hasMore = (aByte & 128) > 0;
         aByte &= 127; //remove leading byte
         result = result | (aByte << (i*7));
         if (!hasMore) break;
      }
      if (result < 0)
         throw new TransportException("negative number read: " + result);
      return result;
   }

   public static void writeVInt(int toWrite, OutputStream os) {
      boolean hasMore;
      do {
         int currentByte = toWrite & 0x0000007F;
         toWrite = toWrite >> 7;
         hasMore = toWrite > 0;
         if (hasMore) {
            currentByte |= 128;
         }
         writeByte(os, currentByte);
      } while (hasMore);
   }

   private static void writeByte(OutputStream os, int currentByte) {
      try {
         os.write(currentByte);
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   private static int nextByte(InputStream is) {
      try {
         int result = is.read();
         if (result < 0) {
            throw new TransportException("Unexpected end of stream " + result);
         }
         return result;
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public static long readVLong(InputStream is) {
      long result = 0;
      for (int i = 0; i < MAX_VLONG_BYTES; i++) {
         long aByte = nextByte(is);
         boolean hasMore = (aByte & 128) > 0;
         aByte &= 127; //remove leading byte
         result = result | (aByte << (i*7));
         if (!hasMore) break;
      }
      if (result < 0)
         throw new TransportException("negative number read: " + result);
      return result;
   }


   public static void writeVLong(long toWrite, OutputStream os) {
      boolean hasMore;
      do {
         long currentByte = toWrite & 0x000000000000007F;
         toWrite = toWrite >> 7;
         hasMore = toWrite > 0;
         if (hasMore) {
            currentByte |= 128;
         }
         writeByte(os, (int)currentByte);
      } while (hasMore);
   }

   public static void main(String[] args) {
      long zero = 0;
      long aByte = 1;
      long shift = 35;
      System.out.println((zero | (aByte << 35)));
      long a = 1l<<18;
      long b = 1l<<17;
      long ab = a * b;
      System.out.println(ab);
   }
}
