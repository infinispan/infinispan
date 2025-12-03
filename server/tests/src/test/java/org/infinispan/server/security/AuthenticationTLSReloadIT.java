package org.infinispan.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.io.FileWatcher;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.CertificateAuthority;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerListener;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 15.1
 **/

@Security
public class AuthenticationTLSReloadIT {

   static Path RELOAD_PATH;

   static {
      try {
         RELOAD_PATH = Files.createTempDirectory(AuthenticationTLSReloadIT.class.getSimpleName());
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
      InfinispanServerExtensionBuilder.config("configuration/AuthenticationServerTLSReloadTest.xml")
         .addListener(new InfinispanServerListener() {
            @Override
            public void before(InfinispanServerDriver driver) {
               try {
                  SERVERS.getServerDriver().getConfiguration().certificateAuthority().exportCertificateWithKey("reload", RELOAD_PATH, "secret".toCharArray(), CertificateAuthority.ExportType.PFX);
               } catch (IOException | GeneralSecurityException e) {
                  throw new RuntimeException(e);
               }
            }
         })
         .property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_BIND, RELOAD_PATH.resolve("reload.pfx") + ":" + "/opt/infinispan/server/conf/reload.pfx")
         .build();


   @ParameterizedTest
   @ArgumentsSource(Common.SaslMechsArgumentProvider.class)
   public void testHotRodReadWrite(String mechanism) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().sniHostName("infinispan.test");
      if (!mechanism.isEmpty()) {
         builder.security().authentication().saslMechanism(mechanism).serverName("infinispan").realm("default").username("all_user").password("all");
      }

      try {
         RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
         cache.put("k1", "v1");
         // Regenerate the server certificate and wait for it to be picked up
         SERVERS.getServerDriver().getConfiguration().certificateAuthority().exportCertificateWithKey("reload", RELOAD_PATH, "secret".toCharArray(), CertificateAuthority.ExportType.PFX);
         Thread.sleep(FileWatcher.SLEEP + 500);
         cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).get();
         assertEquals("v1", cache.get("k1"));
      } catch (HotRodClientException e) {
         // Rethrow if unexpected
         if (!mechanism.isEmpty()) throw e;
      } catch (InterruptedException | GeneralSecurityException | IOException e) {
         throw new RuntimeException(e);
      }
   }
}
