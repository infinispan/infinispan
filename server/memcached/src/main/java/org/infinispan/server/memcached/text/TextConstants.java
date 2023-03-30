package org.infinispan.server.memcached.text;

import java.math.BigInteger;

/**
 * Memcached text protocol utilities.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class TextConstants {
   private TextConstants() {
   }

   public static final String CRLF = "\r\n";
   public static final byte[] CRLFBytes = CRLF.getBytes();
   public static final byte[] END = "END\r\n".getBytes();
   public static final int END_SIZE = END.length;
   public static final byte[] DELETED = "DELETED\r\n".getBytes();
   public static final byte[] NOT_FOUND = "NOT_FOUND\r\n".getBytes();
   public static final byte[] EXISTS = "EXISTS\r\n".getBytes();
   public static final byte[] STORED = "STORED\r\n".getBytes();
   public static final byte[] NOT_STORED = "NOT_STORED\r\n".getBytes();
   public static final byte[] OK = "OK\r\n".getBytes();
   public static final byte[] TOUCHED = "TOUCHED\r\n".getBytes();
   public static final byte[] ERROR = "ERROR\r\n".getBytes();
   public static final String CLIENT_ERROR_BAD_FORMAT = "CLIENT_ERROR bad command line format: ";
   public static final String CLIENT_ERROR_AUTH = "CLIENT_ERROR authentication failed: ";
   public static final String SERVER_ERROR = "SERVER_ERROR ";
   public static final byte[] VALUE = "VALUE ".getBytes();
   public static final int VALUE_SIZE = VALUE.length;
   public static final byte[] ZERO = "0".getBytes();
   public static final byte[] NOREPLY = "noreply\r\n".getBytes();
   public static final byte[] MN = "MN\r\n".getBytes();

   public static final int SPACE = 32;

   public static final BigInteger MAX_UNSIGNED_LONG = new BigInteger("18446744073709551615");
   public static final BigInteger MIN_UNSIGNED = BigInteger.ZERO;

   public static final int MAX_EXPIRATION = 60 * 60 * 24 * 30;
}
