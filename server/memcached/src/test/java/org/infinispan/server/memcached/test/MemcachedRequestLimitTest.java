package org.infinispan.server.memcached.test;

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.PASSWORD;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.USERNAME;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedClient;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.MemcachedStatus;
import org.infinispan.server.memcached.binary.BinaryCommand;
import org.infinispan.server.memcached.binary.BinaryConstants;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.testing.Exceptions;
import org.testng.SkipException;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import net.spy.memcached.internal.OperationFuture;

/**
 * Tests Memcached request that is larger than a configured limit on the Infinispan Server.
 */
@Test(groups = "functional", testName = "server.memcached.test.MemcachedRequestLimitTest")
public class MemcachedRequestLimitTest extends MemcachedSingleNodeTest {
   private MemcachedProtocol protocol;
   private boolean authentication;

   public MemcachedRequestLimitTest protocol(MemcachedProtocol protocol) {
      this.protocol = protocol;
      return this;
   }

   public MemcachedRequestLimitTest authentication(boolean authentication) {
      this.authentication = authentication;
      return this;
   }

   @Override
   public MemcachedProtocol getProtocol() {
      return protocol;
   }

   @Override
   protected boolean withAuthentication() {
      return authentication;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new MemcachedRequestLimitTest().protocol(MemcachedProtocol.BINARY).authentication(false),
            new MemcachedRequestLimitTest().protocol(MemcachedProtocol.TEXT).authentication(false),
            // Until a connection authenticates it is served by a different decoder, which does its own byte
            // accounting and has to enforce the limit the same way the operation decoder does
            new MemcachedRequestLimitTest().protocol(MemcachedProtocol.BINARY).authentication(true),
            new MemcachedRequestLimitTest().protocol(MemcachedProtocol.TEXT).authentication(true),
      };
   }

   @Override
   protected String parameters() {
      return "[" + protocol + ", auth=" + authentication + "]";
   }

   private static final int MAX_CONTENT_LENGTH = 128;
   // Over the limit, but under the 250 character memcached key limit, so it is the limit that rejects it
   private static final int OVERSIZED_KEY_LENGTH = 200;

   public MemcachedRequestLimitTest() {
      // The test handlers install a 1 byte frame decoder, which means a request is never handed to the decoder in the
      // same read as another one. The pipelining test below relies on real reads to spot a request being charged for
      // bytes it already accounted for in an earlier read
      decoderReplay = false;
   }

   @Override
   protected void startServer(MemcachedServer server, MemcachedServerConfigurationBuilder builder) {
      super.startServer(server, builder.maxContentLength(Integer.toString(MAX_CONTENT_LENGTH)));
   }

   public void testKeyTooLong(Method m) throws Exception {
      assertRejected(client.set(k(m, "k".repeat(MAX_CONTENT_LENGTH)), 0, v(m)));
   }

   public void testValueTooLong(Method m) throws Exception {
      assertRejected(client.set(k(m), 0, v(m, "v".repeat(MAX_CONTENT_LENGTH))));
   }

   /**
    * A request under the limit must still be accepted when the read it arrives in ends in the middle of it. The bytes
    * such a request consumed before the split used to be counted twice, so enough pipelined requests to span several
    * reads would see one rejected once a split landed far enough into a request.
    */
   public void testManyPipelinedRequestsNearLimit() throws Exception {
      // Enough requests, each just under the limit, that they cannot all be delivered in a single read
      int opCount = 512;
      List<OperationFuture<Boolean>> futures = new ArrayList<>(opCount);
      List<String> keys = new ArrayList<>(opCount);
      List<String> values = new ArrayList<>(opCount);
      for (int i = 0; i < opCount; ++i) {
         String key = "k" + i;
         // Varying the size keeps the requests from lining up with the read boundaries the same way every time, a
         // read has to end in the middle of a request for the double counting to show
         int valueLength = 84 - key.length() - i % 11;
         keys.add(key);
         values.add(Character.toString('a' + i % 26).repeat(valueLength));
         futures.add(client.set(key, 0, values.get(i)));
      }

      for (int i = 0; i < opCount; ++i) {
         assertEquals(Boolean.TRUE, futures.get(i).get(30, TimeUnit.SECONDS));
      }
      for (int i = 0; i < opCount; ++i) {
         assertEquals(values.get(i), client.get(keys.get(i)));
      }
   }

   public void testExcessDataDoesNotCorruptSubsequentConnection(Method m) throws Exception {
      String oversizedValue = "v".repeat(MAX_CONTENT_LENGTH * 4);
      assertRejected(client.set(k(m), 0, oversizedValue));

      killMemcachedClient(client);
      client = createMemcachedClient(server);

      String key = k(m, "after");
      String value = "smallValue";
      wait(client.set(key, 0, value));
      assertEquals(value, client.get(key));
   }

   /**
    * The authentication decoder has to reject an oversized request as well, a connection must not be able to get
    * around the limit by not authenticating.
    */
   public void testOversizedRequestBeforeAuthentication() throws Exception {
      skipUnlessAuthenticating();
      try (Socket socket = connect()) {
         write(socket, oversizedAuthenticationRequest());
         assertEquals(-1, socket.getInputStream().read(), "The server should have closed the connection");
      }
   }

   /**
    * The authentication decoder must not charge a request twice for the bytes it read before a split either, so a
    * request under the limit has to be accepted even when it arrives in more than one read.
    */
   public void testSplitRequestNearLimitBeforeAuthentication() throws Exception {
      skipUnlessAuthenticating();
      try (Socket socket = connect()) {
         byte[] request = authenticationRequestNearLimit();
         assertTrue(request.length <= MAX_CONTENT_LENGTH, "The request has to be under the limit to be accepted");
         writeInChunks(socket, request);

         InputStream is = socket.getInputStream();
         if (protocol == MemcachedProtocol.TEXT) {
            assertStored(readResponseLine(is));
         } else {
            // SCRAM is a multi step exchange, the server answers the first step with its own challenge
            assertEquals(MemcachedStatus.AUTHN_CONTINUE.getBinary(), readBinaryResponseStatus(is));
         }
      }
   }

   private void skipUnlessAuthenticating() {
      if (!authentication) {
         throw new SkipException("There is no authentication decoder in the pipeline");
      }
   }

   private Socket connect() throws Exception {
      Socket socket = new Socket(server.getHost(), server.getPort());
      socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(timeout));
      // Small writes have to go out on their own for the decoder to see the request arrive in several reads
      socket.setTcpNoDelay(true);
      return socket;
   }

   private static void write(Socket socket, byte[] bytes) throws Exception {
      OutputStream os = socket.getOutputStream();
      os.write(bytes);
      os.flush();
   }

   /**
    * Hands the request over in several writes, so that the decoder has to carry over what the request consumed in
    * each of the reads it is spread over. Coalescing can still merge writes, which only makes the test weaker.
    */
   private static void writeInChunks(Socket socket, byte[] request) throws Exception {
      int chunkSize = 16;
      for (int i = 0; i < request.length; i += chunkSize) {
         write(socket, Arrays.copyOfRange(request, i, Math.min(i + chunkSize, request.length)));
         Thread.sleep(5);
      }
   }

   /**
    * Reads the response line, failing rather than blocking or recursing when the server closed the connection
    * instead of replying.
    */
   private static String readResponseLine(InputStream is) throws Exception {
      StringBuilder sb = new StringBuilder();
      for (int b = is.read(); b != -1; b = is.read()) {
         if (b == '\n') return sb.toString().trim();
         sb.append((char) b);
      }
      throw new AssertionError("The server closed the connection instead of replying, read so far: " + sb);
   }

   private static short readBinaryResponseStatus(InputStream is) throws Exception {
      byte[] header = is.readNBytes(24);
      assertEquals(24, header.length, "The server should have replied with a response header");
      ByteBuffer buffer = ByteBuffer.wrap(header);
      assertEquals(BinaryConstants.MAGIC_RES, buffer.get());
      return buffer.getShort(6);
   }

   private byte[] oversizedAuthenticationRequest() {
      String key = "k".repeat(OVERSIZED_KEY_LENGTH);
      if (protocol == MemcachedProtocol.TEXT) {
         // The text authentication decoder takes the credentials as the value of a set operation
         return ("set " + key + " 0 0 5\r\nhello\r\n").getBytes(StandardCharsets.US_ASCII);
      }
      return binarySaslAuth(key, "");
   }

   private byte[] authenticationRequestNearLimit() {
      if (protocol == MemcachedProtocol.TEXT) {
         String credentials = USERNAME + " " + PASSWORD;
         String suffix = " 0 0 " + credentials.length() + "\r\n" + credentials + "\r\n";
         // Pad the key so that the request stops just short of the limit
         String key = "k".repeat(MAX_CONTENT_LENGTH - "set ".length() - suffix.length());
         return ("set " + key + suffix).getBytes(StandardCharsets.US_ASCII);
      }
      return binarySaslAuth("SCRAM-SHA-256", "n,,n=" + USERNAME + ",r=0123456789abcdef");
   }

   private static byte[] binarySaslAuth(String mech, String data) {
      ByteBuffer buffer = ByteBuffer.allocate(24 + mech.length() + data.length());
      buffer.put(BinaryConstants.MAGIC_REQ);
      buffer.put(BinaryCommand.SASL_AUTH.opCode());
      buffer.putShort((short) mech.length()); // key length
      buffer.put((byte) 0); // extras length
      buffer.put((byte) 0); // data type
      buffer.putShort((short) 0); // vbucket id
      buffer.putInt(mech.length() + data.length()); // total body length
      buffer.putInt(0); // opaque
      buffer.putLong(0); // cas
      buffer.put(mech.getBytes(StandardCharsets.US_ASCII));
      buffer.put(data.getBytes(StandardCharsets.US_ASCII));
      return buffer.array();
   }

   /**
    * The server rejects a request over the limit by closing the connection, which the client reports as a failed
    * operation on an unauthenticated connection and as a cancelled one otherwise, so only the status is common.
    */
   private static void assertRejected(OperationFuture<Boolean> f) throws Exception {
      try {
         f.get(10, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
         Exceptions.assertException(ExecutionException.class, CancellationException.class, e);
      }
      assertFalse(f.getStatus().isSuccess(), "The operation should have been rejected");
   }
}
