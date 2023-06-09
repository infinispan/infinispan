package org.infinispan.quarkus.server;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Test to ensure that the native quarkus CLI is able to execute.
 *
 * @author Ryan Emerson
 * @since 12.1
 */
public class NativeCliIT {
   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .build();

   @Test
   public void testCliBatch() throws Exception {
      RestClient client = SERVERS.rest().create();
      RestCacheManagerClient cm = client.cacheManager("default");
      RestResponse restResponse = sync(cm.healthStatus());
      assertEquals(200, restResponse.getStatus());
      assertEquals("HEALTHY", restResponse.getBody());

      String cliPath = resource("ispn-cli");
      String batchFile = resource("batch.file");
      String hostname = SERVERS.getTestServer().getDriver().getServerAddress(0).getHostAddress() + ":11222";
      String cliCmd = String.format("%s --connect %s --file=%s", cliPath, hostname, batchFile);

      ProcessBuilder pb = new ProcessBuilder("bash", "-c", cliCmd);
      pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
      Process p = pb.start();
      StringBuilder sb = new StringBuilder();
      new Thread(() -> {
         try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
               sb.append(line);
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }).start();

      p.waitFor(5, TimeUnit.SECONDS);
      String stderr = sb.toString();
      if (!stderr.isEmpty()) {
         System.err.println(stderr);
         fail("Unexpected CLI output in stderr");
      }
      assertEquals(0, p.exitValue());
      restResponse = sync(client.cache("mybatch").exists());
      assertEquals(204, restResponse.getStatus());
   }

   private String resource(String name) throws Exception {
      return Paths.get(NativeCliIT.class.getClassLoader().getResource(name).toURI()).toString();
   }
}
