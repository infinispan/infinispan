package org.infinispan.persistence.remote.shared;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.Properties;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.RemoteStoreFunctionalTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfigurationBuilder;
import org.infinispan.server.core.security.simple.SimpleSaslAuthenticator;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "persistence.remote.shared.RemoteSharedCacheManagerSSLTest", groups = "functional")
public class RemoteSharedCacheManagerSSLTest extends RemoteStoreFunctionalTest {

   private static final String REMOTE_CONTAINER_NAME = "shared-remote-container";

   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @BeforeClass
   protected void setupBefore() {
      localCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());

      SimpleSaslAuthenticator ssa = new SimpleSaslAuthenticator();
      HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();
      serverBuilder
            .ssl()
            .enable()
            .requireClientAuth(true)
            .keyStoreFileName(TestCertificates.certificate("server"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyAlias("server")
            .trustStoreFileName(TestCertificates.certificate("trust"))
            .trustStorePassword(TestCertificates.KEY_PASSWORD);
      serverBuilder
            .authentication()
            .enable()
            .sasl()
            .serverName("localhost")
            .addAllowedMech("EXTERNAL")
            .authenticator(ssa);

      hrServer = HotRodClientTestingUtil.startHotRodServer(localCacheManager, serverBuilder);
   }

   @AfterClass
   protected void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      localCacheManager.stop();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(boolean start, GlobalConfigurationBuilder global, ConfigurationBuilder cb) {
      // We configure all the security properties to work correctly.
      // See: https://docs.jboss.org/infinispan/15.0/apidocs/org/infinispan/client/hotrod/configuration/package-summary.html
      Properties properties = new Properties();
      properties.setProperty("infinispan.client.hotrod.use_ssl", "true");
      properties.setProperty("infinispan.client.hotrod.key_store_file_name", TestCertificates.certificate("client"));
      properties.setProperty("infinispan.client.hotrod.key_store_password", new String(TestCertificates.KEY_PASSWORD));
      properties.setProperty("infinispan.client.hotrod.trust_store_file_name", TestCertificates.certificate("ca"));
      properties.setProperty("infinispan.client.hotrod.trust_store_password", new String(TestCertificates.KEY_PASSWORD));
      properties.setProperty("infinispan.client.hotrod.ssl_hostname_validation", "false");
      properties.setProperty("infinispan.client.hotrod.use_auth", "true");
      properties.setProperty("infinispan.client.hotrod.sasl_mechanism", "EXTERNAL");

      RemoteContainersConfigurationBuilder rccb = global.addModule(RemoteContainersConfigurationBuilder.class);
      rccb.addRemoteContainer(REMOTE_CONTAINER_NAME)
            .uri(String.format("hotrod://localhost:%d", hrServer.getPort()))
            .properties(properties);
      return super.createCacheManager(start, global, cb);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
                                                                    String cacheName, boolean preload) {
      persistence
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName("")
            .preload(preload)
            // local cache encoding is object where as server is protostream so we can't be segmented
            .segmented(false)
            .remoteCacheContainer(REMOTE_CONTAINER_NAME);
      return persistence;
   }
}
