package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * End to end verification of ISPN-18130: a Hot Rod operation rejected for exceeding {@code max-content-length} must
 * leave no trace in the cache. The remainder of the rejected request used to be decoded as if it were the continuation
 * of it, so the first byte of the payload was read as the length of the value and a truncated entry was stored.
 *
 * @since 16.3
 */
public class HotRodMaxContentLengthIT {

   private static final int MAX_CONTENT_LENGTH = 1024 * 1024;
   /**
    * Every byte of the payload is a valid single byte vInt, so any of them left in the decoder buffer would be read as
    * the length of a shorter value. The first one is {@code 'O'}, i.e. 79, which is the length the bug reported.
    */
   private static final String OVERSIZE_MARKER = "OVERSIZE-SHOULD-FAIL-";

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/HotRodMaxContentLengthTest.xml")
               .numServers(1)
               .build();

   @Test
   public void testOversizedValueIsNotStored() {
      RemoteCache<String, byte[]> cache = SERVERS.hotrod().withClientConfiguration(clientConfiguration()).create();
      String key = "oversized-key";

      assertThrows(HotRodClientException.class, () -> cache.put(key, oversizedValue(2 * MAX_CONTENT_LENGTH)));

      // A fresh connection, since the rejected one has been closed by the server
      RemoteCache<String, byte[]> verifier =
            SERVERS.hotrod().withClientConfiguration(clientConfiguration()).get(cache.getName());
      assertNull(verifier.get(key), "A truncated entry was stored for the rejected request");
      assertEquals(0, verifier.size(), "The rejected request must not have stored anything");
   }

   /**
    * The entry written by a valid operation must survive an oversized one that follows it on the same connection.
    */
   @Test
   public void testOversizedValueDoesNotOverwritePreviousEntry() {
      RemoteCache<String, byte[]> cache = SERVERS.hotrod().withClientConfiguration(clientConfiguration()).create();
      String key = "overwritten-key";
      byte[] value = "expected-value".getBytes(StandardCharsets.UTF_8);

      cache.put(key, value);
      assertThrows(HotRodClientException.class, () -> cache.put(key, oversizedValue(2 * MAX_CONTENT_LENGTH)));

      RemoteCache<String, byte[]> verifier =
            SERVERS.hotrod().withClientConfiguration(clientConfiguration()).get(cache.getName());
      assertEquals(new String(value, StandardCharsets.UTF_8),
            new String(verifier.get(key), StandardCharsets.UTF_8), "The rejected request overwrote the entry");
      assertEquals(1, verifier.size());
   }

   private static ConfigurationBuilder clientConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // Retrying would just have the server reject and close again, hiding the original failure behind a timeout
      builder.maxRetries(0);
      return builder;
   }

   private static byte[] oversizedValue(int size) {
      byte[] value = new byte[size];
      byte[] marker = OVERSIZE_MARKER.getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < value.length; i++) {
         value[i] = marker[i % marker.length];
      }
      return value;
   }
}
