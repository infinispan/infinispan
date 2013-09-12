package org.infinispan.it.compatibility;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests compatibility of Memcached, using a different client to SpyMemcached,
 * and Hot Rod.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.compatibility.CustomMemcachedHotRodTest")
public class CustomMemcachedHotRodTest extends AbstractInfinispanTest {

   final static String CACHE_NAME = "memcachedCache";

   CompatibilityCacheFactory<String, String> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<String, String>(
            CACHE_NAME, new StringMarshaller(), CacheMode.LOCAL).setup();
   }

   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testHotRodPutMemcachedGet() throws IOException {
      final String key = "1";

      // 1. Put with Hot Rod
      RemoteCache<String, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Read with Memcached
      MemcachedClient memcached =
            new MemcachedClient("localhost", cacheFactory.getMemcachedPort());
      try {
         assertEquals("v1".getBytes(), memcached.getBytes(key));
      } finally {
         memcached.close();
      }
   }

   public void testMemcachedPutGet() throws IOException {
      final String key = "1";

      MemcachedClient memcached =
            new MemcachedClient("localhost", cacheFactory.getMemcachedPort());
      try {
         memcached.set(key, "v1");
         assertEquals("v1", memcached.get(key));
      } finally {
         memcached.close();
      }
   }

   /**
    * Alternative Memcached client to SpyMemcached.
    *
    * @author Martin Gencur
    */
   static class MemcachedClient {

      private static final int DEFAULT_TIMEOUT = 10000;
      private static final String DEFAULT_ENCODING = "UTF-8";

      private String encoding;
      private Socket socket;
      private PrintWriter out;
      private InputStream input;

      public MemcachedClient(String host, int port) throws IOException {
         this(DEFAULT_ENCODING, host, port, DEFAULT_TIMEOUT);
      }

      public MemcachedClient(String enc, String host, int port, int timeout) throws IOException {
         encoding = enc;
         socket = new Socket(host, port);
         socket.setSoTimeout(timeout);
         out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), encoding));
         input = socket.getInputStream();
      }

      public String get(String key) throws IOException {
         byte[] data = getBytes(key);
         return (data == null) ? null : new String(data, encoding);
      }

      public byte[] getBytes(String key) throws IOException {
         writeln("get " + key);
         flush();
         String valueStr = readln();
         if (valueStr.startsWith("VALUE")) {
            String[] value = valueStr.split(" ");
            assertEquals(key, value[1]);
            int size = new Integer(value[3]);
            byte[] ret = read(size);
            assertEquals('\r', read());
            assertEquals('\n', read());
            assertEquals("END", readln());
            return ret;
         } else {
            return null;
         }
      }

      public void set(String key, String value) throws IOException {
         writeln("set " + key + " 0 0 " + value.getBytes(encoding).length);
         writeln(value);
         flush();
         assertEquals("STORED", readln());
      }

      private byte[] read(int len) throws IOException {
         try {
            byte[] ret = new byte[len];
            input.read(ret, 0, len);
            return ret;
         } catch (SocketTimeoutException ste) {
            return null;
         }
      }

      private byte read() throws IOException {
         try {
            return (byte) input.read();
         } catch (SocketTimeoutException ste) {
            return -1;
         }
      }

      private String readln() throws IOException {
         byte[] buf = new byte[512];
         int maxlen = 512;
         int read = 0;
         buf[read] = read();
         while (buf[read] != '\n') {
            read++;
            if (read == maxlen) {
               maxlen += 512;
               buf = Arrays.copyOf(buf, maxlen);
            }
            buf[read] = read();
         }
         if (read == 0) {
            return "";
         }
         if (buf[read - 1] == '\r') {
            read--;
         }
         buf = Arrays.copyOf(buf, read);
         return new String(buf, encoding);
      }

      private void writeln(String str) {
         out.print(str + "\r\n");
      }

      private void flush() {
         out.flush();
      }

      public void close() throws IOException {
         socket.close();
      }
   }

   static class StringMarshaller extends AbstractMarshaller {

      private static final Charset DEFAULT_ENCODING = Charset.forName("UTF-8");

      @Override
      protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
         byte[] bytes = ((String) o).getBytes(DEFAULT_ENCODING);
         return new ByteBufferImpl(bytes, 0, bytes.length);
      }

      @Override
      public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
         return new String(buf, DEFAULT_ENCODING);
      }

      @Override
      public boolean isMarshallable(Object o) throws Exception {
         return o instanceof String;
      }

   }

}
