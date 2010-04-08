package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
@Test(testName = "client.hotrod.VHelperTest", groups = "unit, functional", enabled = false, description = "TODO To be re-enabled when we have a multithreaded HotRod server impl")
public class VHelperTest {

   public void testString2Byte() {
      str2byte("00000001", (byte) 1);
      str2byte("00001001", (byte) 9);
      str2byte("01111111", (byte) 127);
      str2byte("11111111", (byte) -1);
      str2byte("11111110", (byte) -2);
      str2byte("10000001", (byte) -127);
      str2byte("10000000", (byte) -128);
      str2byte("11111011", (byte) -5);
   }

   public void testByte2String() {
      byte2str((byte) 9, "00001001");
      byte2str((byte) 1, "00000001");
      byte2str((byte) 127, "01111111");
      byte2str((byte) -1, "11111111");
      byte2str((byte) -2, "11111110");
      byte2str((byte) -127, "10000001");
      byte2str((byte) -128, "10000000");
      byte2str((byte) -5, "11111011");
   }

   public void testReadVInt1() {
      assert 0 == VHelper.readVInt(getInputStream((byte) 0));
      assert 1 == VHelper.readVInt(getInputStream((byte) 1));
      assert 2 == VHelper.readVInt(getInputStream((byte) 2));

      assert 127 == VHelper.readVInt(getInputStream((byte) 127));


      byte[] nr128 = new byte[]{str2byte("10000000"), str2byte("00000001")};
      assert 128 == VHelper.readVInt(getInputStream(nr128));


      byte[] nr129 = new byte[]{str2byte("10000001"), str2byte("00000001")};
      assert 129 == VHelper.readVInt(getInputStream(nr129));

      byte[] nr130 = new byte[]{str2byte("10000010"), str2byte("00000001")};
      assert 130 == VHelper.readVInt(getInputStream(nr130));

      byte[] nr16383 = new byte[]{str2byte("11111111"), str2byte("01111111")};
      assert 16383 == VHelper.readVInt(getInputStream(nr16383));

      byte[] nr16384 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("00000001")};
      assert 16384 == VHelper.readVInt(getInputStream(nr16384));

      byte[] nr16385 = new byte[]{str2byte("10000001"), str2byte("10000000"), str2byte("00000001")};
      assert 16385 == VHelper.readVInt(getInputStream(nr16385));

      byte[] nr16393 = new byte[]{str2byte("10001001"), str2byte("10000000"), str2byte("00000001")};
      assert 16393 == VHelper.readVInt(getInputStream(nr16393));

      byte[] nr2pow28 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000001")};
      assert (1 << 28) == VHelper.readVInt(getInputStream(nr2pow28));

      byte[] nr2pow30 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000100")};
      assert (1 << 30) == VHelper.readVInt(getInputStream(nr2pow30));

      byte[] nr2pow30plus2 = new byte[]{str2byte("10000010"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000100")};
      assert (1 << 30) + 2 == VHelper.readVInt(getInputStream(nr2pow30plus2));
   }

   public void testWriteVint() {
      String[] written = writeVInt(0);
      assert Arrays.equals(written, new String[]{"00000000"});

      written = writeVInt(1);
      assert Arrays.equals(written, new String[]{"00000001"});

      written = writeVInt(127);
      assert Arrays.equals(written, new String[]{"01111111"});

      written = writeVInt(129);
      assert Arrays.equals(written, new String[]{"10000001", "00000001"});

      written = writeVInt(130);
      assert Arrays.equals(written, new String[]{"10000010", "00000001"});

      written = writeVInt(16383);
      assert Arrays.equals(written, new String[]{"11111111", "01111111"});

      written = writeVInt(16384);
      assert Arrays.equals(written, new String[]{"10000000", "10000000", "00000001"});

      written = writeVInt(16385);
      assert Arrays.equals(written, new String[]{"10000001", "10000000", "00000001"});

      written = writeVInt(1 << 28);
      assert Arrays.equals(written, new String[]{"10000000", "10000000", "10000000", "10000000", "00000001"});

      written = writeVInt(1 << 30);
      assert Arrays.equals(written, new String[]{"10000000", "10000000", "10000000", "10000000", "00000100"});

      written = writeVInt((1 << 30) + 2);
      assert Arrays.equals(written, new String[]{"10000010", "10000000", "10000000", "10000000", "00000100"});
   }

   public void testReadVLong() {
      assert 0 == VHelper.readVLong(getInputStream((byte) 0));
      assert 1 == VHelper.readVLong(getInputStream((byte) 1));
      assert 2 == VHelper.readVLong(getInputStream((byte) 2));

      assert 127 == VHelper.readVLong(getInputStream((byte) 127));


      byte[] nr128 = new byte[]{str2byte("10000000"), str2byte("00000001")};
      assert 128 == VHelper.readVLong(getInputStream(nr128));


      byte[] nr129 = new byte[]{str2byte("10000001"), str2byte("00000001")};
      assert 129 == VHelper.readVLong(getInputStream(nr129));

      byte[] nr130 = new byte[]{str2byte("10000010"), str2byte("00000001")};
      assert 130 == VHelper.readVLong(getInputStream(nr130));

      byte[] nr16383 = new byte[]{str2byte("11111111"), str2byte("01111111")};
      assert 16383 == VHelper.readVLong(getInputStream(nr16383));

      byte[] nr16384 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("00000001")};
      assert 16384 == VHelper.readVLong(getInputStream(nr16384));

      byte[] nr16385 = new byte[]{str2byte("10000001"), str2byte("10000000"), str2byte("00000001")};
      assert 16385 == VHelper.readVLong(getInputStream(nr16385));

      byte[] nr2pow28 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000001")};
      assert (1 << 28) == VHelper.readVLong(getInputStream(nr2pow28));

      byte[] nr2pow30 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000100")};
      assert (1 << 30) == VHelper.readVLong(getInputStream(nr2pow30));

      byte[] nr2pow30plus2 = new byte[]{str2byte("10000010"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000100")};
      assert (1 << 30) + 2 == VHelper.readVLong(getInputStream(nr2pow30plus2));

      byte[] nr2pow35 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000001")};
      long expected = 1l << 35;
      long obtained = VHelper.readVLong(getInputStream(nr2pow35));
      assert expected == obtained : "expected " + expected + " but received " + obtained;


      byte[] nr2pow42 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000001")};
      expected = (long) (1 << 22) * (long) (1 << 20);
      obtained = VHelper.readVLong(getInputStream(nr2pow42));
      assert expected == obtained : "Expected " + expected + " but obtained " + obtained;

      byte[] nr2pow56 = new byte[]{str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("10000000"), str2byte("00000001")};
      expected = 1l << 56;
      obtained = VHelper.readVLong(getInputStream(nr2pow56));
      assert expected == obtained : "Expected " + expected + " but obtained " + obtained;
   }

   public void testWriteVLong() {
      String[] written = writeVLong(0);
      assert Arrays.equals(written, new String[]{"00000000"});

      written = writeVLong(1);
      assert Arrays.equals(written, new String[]{"00000001"});

      written = writeVLong(127);
      assert Arrays.equals(written, new String[]{"01111111"});

      written = writeVLong(129);
      assert Arrays.equals(written, new String[]{"10000001", "00000001"});

      written = writeVLong(130);
      assert Arrays.equals(written, new String[]{"10000010", "00000001"});

      written = writeVLong(16383);
      assert Arrays.equals(written, new String[]{"11111111", "01111111"});

      written = writeVLong(16384);
      assert Arrays.equals(written, new String[]{"10000000", "10000000", "00000001"});

      written = writeVLong(16385);
      assert Arrays.equals(written, new String[]{"10000001", "10000000", "00000001"});

      written = writeVLong(1 << 28);
      assert Arrays.equals(written, new String[]{"10000000", "10000000", "10000000", "10000000", "00000001"});

      written = writeVLong(1 << 30);
      assert Arrays.equals(written, new String[]{"10000000", "10000000", "10000000", "10000000", "00000100"});

      written = writeVLong((1 << 30) + 2);
      assert Arrays.equals(written, new String[]{"10000010", "10000000", "10000000", "10000000", "00000100"});

      written = writeVLong((1l << 56) + 2);
      assert Arrays.equals(written, new String[]{"10000010", "10000000", "10000000", "10000000", "10000000", "10000000", "10000000", "10000000", "00000001"});
   }


   private String[] writeVInt(int toWrite) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(5);
      VHelper.writeVInt(toWrite, baos);
      byte[] result = baos.toByteArray();
      return toStringArray(result);
   }

   private String[] writeVLong(long toWrite) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(9);
      VHelper.writeVLong(toWrite, baos);
      byte[] result = baos.toByteArray();
      return toStringArray(result);
   }

   private String[] toStringArray(byte[] result) {
      String[] resultStr = new String[result.length];
      for (int i = 0; i < resultStr.length; i++) {
         resultStr[i] = byte2str(result[i]);
      }
      return resultStr;
   }


   private InputStream getInputStream(byte... bytes) {
      return new ByteArrayInputStream(bytes);
   }


   private byte str2byte(String str, byte... check) {
      assert str.length() == 8;
      byte result = 0;
      for (int i = 7; i >= 0; i--) {
         assert str.charAt(i) == '0' || str.charAt(i) == '1';
         boolean isOne = str.charAt(i) == '1';
         byte mask = isOne ? ((byte) (1 << (7 - i))) : (byte) 0;
         result |= mask;
      }
      if (check != null && check.length > 0) {
         assert result == check[0] : "Expected " + check[0] + " but received " + result;
      }
      return result;
   }

   private String byte2str(byte aByte, String... expectedValue) {
      String result = "";
      for (int i = 0; i <= 7; i++) {
         byte mask = (byte) (1 << (7 - i));
         boolean isOne = (mask & aByte) != 0;
         result += isOne ? "1" : "0";
      }
      if (expectedValue != null && expectedValue.length > 0) {
         assert result.equals(expectedValue[0]) : "Expected " + expectedValue[0] + " but received " + result;
      }
      return result;
   }
}
