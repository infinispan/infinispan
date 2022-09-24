package org.infinispan.server.hotrod;

import static org.testng.AssertJUnit.assertEquals;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.testng.annotations.Test;

/**
 * @since 14.0
 **/
@Test(groups = "functional", testName = "server.hotrod.InetAddressWithNetMaskTest")
public class InetAddressWithNetMaskTest {

   public void testNetmasksOverrides() throws UnknownHostException {
      // Private/reserved ranges
      // 10.0.0.0/8
      check((byte) 10, (byte) 1, (byte) 2, (byte) 3, (short) 21, (short) 8);
      // 100.64.0.0/10
      check((byte) 100, (byte) 64, (byte) 2, (byte) 3, (short) 21, (short) 10);
      // 169.254.0.0/16
      check((byte) 169, (byte) 254, (byte) 2, (byte) 3, (short) 21, (short) 16);
      // 172.16.0.0/12
      check((byte) 172, (byte) 30, (byte) 2, (byte) 3, (short) 21, (short) 12);
      // 192.168.0.0/16
      check((byte) 192, (byte) 168, (byte) 2, (byte) 3, (short) 21, (short) 16);
      // 240.0.0.0/4 (Class E)
      check((byte) 250, (byte) 1, (byte) 2, (byte) 3, (short) 21, (short) 4);

      // Public ranges
      // Below 10.0.0.0/8
      check((byte) 8, (byte) 1, (byte) 2, (byte) 3, (short) 21, (short) 21);
      // Between 10.0.0.0/8 and 100.64.0.0/10
      check((byte) 100, (byte) 63, (byte) 2, (byte) 3, (short) 21, (short) 21);
      // Between 100.64.0.0/10 and 169.254.0.0/16
      check((byte) 100, (byte) 130, (byte) 2, (byte) 3, (short) 21, (short) 21);
      // Between 169.254.0.0/16 and 172.16.0.0/12
      check((byte) 170, (byte) 1, (byte) 2, (byte) 3, (short) 21, (short) 21);
      // Between 172.16.0.0/12 and 192.168.0.0
      check((byte) 172, (byte) 32, (byte) 16, (byte) 3, (short) 21, (short) 21);
      // Between 192.168.0.0 and 240.0.0.0/4
      check((byte) 223, (byte) 32, (byte) 16, (byte) 8, (short) 21, (short) 21);
   }

   private void check(byte a, byte b, byte c, byte d, short prefix, short expected) throws UnknownHostException {
      InetAddress address = InetAddress.getByAddress(new byte[]{a, b, c, d});
      MultiHomedServerAddress.InetAddressWithNetMask addressWithNetMask = new MultiHomedServerAddress.InetAddressWithNetMask(address, prefix, true);
      assertEquals(expected, addressWithNetMask.prefixLength);
      addressWithNetMask = new MultiHomedServerAddress.InetAddressWithNetMask(address, prefix, false);
      assertEquals(prefix, addressWithNetMask.prefixLength);
   }
}
