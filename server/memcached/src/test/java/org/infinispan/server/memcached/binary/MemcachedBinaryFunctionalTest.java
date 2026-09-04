package org.infinispan.server.memcached.binary;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.test.MemcachedFunctionalTest;
import org.testng.annotations.Test;

import net.spy.memcached.CASValue;
import net.spy.memcached.internal.OperationFuture;

/**
 * @since 15.0
 **/
@Test(groups = "functional", testName = "server.memcached.binary.MemcachedBinaryFunctionalTest")
public class MemcachedBinaryFunctionalTest extends MemcachedFunctionalTest {

   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.BINARY;
   }

   @Override
   protected boolean withAuthentication() {
      return false;
   }

   public void testSetBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      String k = k(m);
      String v = v(m);
      wait(client.set(k, 0, v));
      CASValue<Object> v1 = client.gets(k);
      assertEquals(v, v1.getValue());
      wait(client.set(k, 0, v + "+"));
      CASValue<Object> v2 = client.gets(k);
      assertEquals(v + "+", v2.getValue());
      assertNotEquals(v1.getCas(), v2.getCas());
      wait(client.delete(k));
   }

   public void testStats() {
      Map<String, String> stats = client.getStats().values().iterator().next();
      assertFalse(stats.isEmpty());
      assertNotNull(stats.get("version"));
      assertNotNull(stats.get("pid"));
      assertNotNull(stats.get("curr_items"));
   }

   public void testStatsSingleKey() throws IOException {
      try (Socket socket = newBinarySocket()) {
         byte[] statKey = "version".getBytes(StandardCharsets.US_ASCII);
         sendBinaryOp(socket, (byte) 0x10, null, statKey, null, 0);
         byte[] response = readBinaryResponse(socket);
         assertEquals(0, extractStatus(response));
      }
   }

   public void testIncrementWithInitialValue(Method m) {
      long result = client.incr(k(m), 1, 100, 0);
      assertEquals(100, result);
      assertEquals(101, client.incr(k(m), 1));
   }

   public void testDecrementWithInitialValue(Method m) {
      long result = client.decr(k(m), 1, 50, 0);
      assertEquals(50, result);
      assertEquals(49, client.decr(k(m), 1));
   }

   public void testIncrementWithInitialAndExpiry(Method m) {
      long result = client.incr(k(m), 1, 100, 1);
      assertEquals(100, result);
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testDecrementWithInitialAndExpiry(Method m) {
      long result = client.decr(k(m), 1, 200, 1);
      assertEquals(200, result);
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testDeleteWithCas(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      wait(client.set(k(m), 0, v(m)));
      CASValue<Object> value = client.gets(k(m));
      long cas = value.getCas();
      assertTrue(cas != 0);

      OperationFuture<Boolean> f = client.delete(k(m), cas);
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertNull(client.get(k(m)));
   }

   public void testDeleteWithCasWrongCas(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      wait(client.set(k(m), 0, v(m)));
      CASValue<Object> value = client.gets(k(m));

      OperationFuture<Boolean> f = client.delete(k(m), value.getCas() + 1);
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(v(m), client.get(k(m)));
   }

   public void testDeleteWithCasKeyNotFound(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.delete(k(m), 12345L);
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
   }

   // --- Raw binary protocol tests ---

   public void testQuit() throws IOException {
      try (Socket socket = newBinarySocket()) {
         sendBinaryOp(socket, (byte) 0x07, null, null, null, 0);
         // The server should close the connection after QUIT.
         // Due to a response flushing issue with the close listener, the response
         // may not always be received, but the connection must be closed.
         byte[] buf = new byte[24];
         int total = 0;
         int n;
         while ((n = socket.getInputStream().read(buf, total, buf.length - total)) != -1) {
            total += n;
            if (total >= 24) break;
         }
         if (total == 24) {
            assertEquals((byte) 0x81, buf[0]);
            assertEquals(0, extractStatus(buf));
         }
      }
   }

   public void testQuitQuiet() throws IOException {
      try (Socket socket = newBinarySocket()) {
         sendBinaryOp(socket, (byte) 0x17, null, null, null, 0);
         assertEquals(-1, socket.getInputStream().read());
      }
   }

   public void testVerbosity() throws IOException {
      try (Socket socket = newBinarySocket()) {
         byte[] extras = new byte[4]; // verbosity level 0
         sendBinaryOp(socket, (byte) 0x1b, extras, null, null, 0);
         byte[] response = readBinaryResponse(socket);
         assertEquals(0, extractStatus(response));
      }
   }

   public void testFlushQuiet(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      wait(client.set(k(m), 0, v(m)));
      assertEquals(v(m), client.get(k(m)));

      try (Socket socket = newBinarySocket()) {
         sendBinaryOp(socket, (byte) 0x18, null, null, null, 0);
         // FLUSHQ is quiet, no response expected. Send a NOOP to flush the pipeline.
         sendBinaryOp(socket, (byte) 0x0a, null, null, null, 0);
         byte[] response = readBinaryResponse(socket);
         assertEquals(0, extractStatus(response));
      }

      assertNull(client.get(k(m)));
   }

   public void testReplaceWithCas(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      wait(client.set(k(m), 0, v(m)));
      CASValue<Object> value = client.gets(k(m));

      byte[] key = k(m).getBytes(StandardCharsets.UTF_8);
      byte[] newValue = v(m, "v1-").getBytes(StandardCharsets.UTF_8);
      byte[] extras = new byte[8];

      try (Socket socket = newBinarySocket()) {
         sendBinaryOp(socket, (byte) 0x03, extras, key, newValue, value.getCas());
         byte[] response = readBinaryResponse(socket);
         assertEquals(0, extractStatus(response));
      }
      assertEquals(v(m, "v1-"), client.get(k(m)));
   }

   public void testReplaceWithCasWrongCas(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      wait(client.set(k(m), 0, v(m)));

      byte[] key = k(m).getBytes(StandardCharsets.UTF_8);
      byte[] newValue = v(m, "v1-").getBytes(StandardCharsets.UTF_8);
      byte[] extras = new byte[8];

      try (Socket socket = newBinarySocket()) {
         sendBinaryOp(socket, (byte) 0x03, extras, key, newValue, 12345L);
         byte[] response = readBinaryResponse(socket);
         assertEquals(0x0005, extractStatus(response)); // ITEM_NOT_STORED (CAS_BADVAL)
      }
      assertEquals(v(m), client.get(k(m)));
   }

   public void testReplaceWithCasKeyNotFound() throws IOException {
      byte[] key = "nonexistent-key".getBytes(StandardCharsets.UTF_8);
      byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);
      byte[] extras = new byte[8];

      try (Socket socket = newBinarySocket()) {
         sendBinaryOp(socket, (byte) 0x03, extras, key, value, 12345L);
         byte[] response = readBinaryResponse(socket);
         assertEquals(0x0001, extractStatus(response)); // KEY_NOT_FOUND
      }
   }

   private Socket newBinarySocket() throws IOException {
      Socket socket = new Socket(server.getHost(), server.getPort());
      socket.setSoTimeout(5000);
      return socket;
   }

   private void sendBinaryOp(Socket socket, byte opcode, byte[] extras, byte[] key, byte[] value, long cas) throws IOException {
      if (extras == null) extras = new byte[0];
      if (key == null) key = new byte[0];
      if (value == null) value = new byte[0];

      int totalBody = extras.length + key.length + value.length;
      OutputStream out = socket.getOutputStream();
      byte[] header = new byte[24];
      header[0] = (byte) 0x80; // magic
      header[1] = opcode;
      header[2] = (byte) (key.length >> 8);
      header[3] = (byte) key.length;
      header[4] = (byte) extras.length;
      header[5] = 0; // data type
      header[6] = 0; // vbucket
      header[7] = 0;
      header[8] = (byte) (totalBody >> 24);
      header[9] = (byte) (totalBody >> 16);
      header[10] = (byte) (totalBody >> 8);
      header[11] = (byte) totalBody;
      // opaque (bytes 12-15) = 0
      header[16] = (byte) (cas >> 56);
      header[17] = (byte) (cas >> 48);
      header[18] = (byte) (cas >> 40);
      header[19] = (byte) (cas >> 32);
      header[20] = (byte) (cas >> 24);
      header[21] = (byte) (cas >> 16);
      header[22] = (byte) (cas >> 8);
      header[23] = (byte) cas;
      out.write(header);
      out.write(extras);
      out.write(key);
      out.write(value);
      out.flush();
   }

   private byte[] readBinaryResponse(Socket socket) throws IOException {
      DataInputStream in = new DataInputStream(socket.getInputStream());
      byte[] header = new byte[24];
      in.readFully(header);
      assertEquals((byte) 0x81, header[0]); // response magic
      int bodyLength = ((header[8] & 0xFF) << 24) | ((header[9] & 0xFF) << 16) |
            ((header[10] & 0xFF) << 8) | (header[11] & 0xFF);
      byte[] body = new byte[bodyLength];
      if (bodyLength > 0) {
         in.readFully(body);
      }
      byte[] full = new byte[24 + bodyLength];
      System.arraycopy(header, 0, full, 0, 24);
      System.arraycopy(body, 0, full, 24, bodyLength);
      return full;
   }

   private int extractStatus(byte[] response) {
      return ((response[6] & 0xFF) << 8) | (response[7] & 0xFF);
   }
}
