package test.org.infinispan.spring.starter.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.RealmCallback;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ClusterConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteCacheManagerAutoConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(
      classes = {
            InfinispanRemoteAutoConfiguration.class,
            InfinispanRemoteCacheManagerAutoConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off"
      })
@TestPropertySource(locations = "classpath:test-application.properties")
public class ApplicationPropertiesTest {

   @Autowired
   private RemoteCacheManager remoteCacheManager;

   @BeforeAll
   public static void initCertificates() throws GeneralSecurityException, IOException {
      CertUtil.initCertificates("superKeyStoreFile.pfx", "superTrustFile.pfx", "superAliasKey");
   }

   @Test
   public void testDefaultClient() throws Exception {
      //when
      Configuration configuration = remoteCacheManager.getConfiguration();
      String hostObtainedFromPropertiesFile = configuration.servers().get(0).host();
      int portObtainedFromPropertiesFile = configuration.servers().get(0).port();

      configuration.asyncExecutorFactory().factoryClass();

      // properties
      assertThat(hostObtainedFromPropertiesFile).isEqualTo("180.567.112.333");
      assertThat(portObtainedFromPropertiesFile).isEqualTo(6668);
      assertThat(configuration.tcpNoDelay()).isFalse();
      assertThat(configuration.tcpKeepAlive()).isTrue();
      assertThat(configuration.clientIntelligence()).isEqualTo(ClientIntelligence.TOPOLOGY_AWARE);
      assertThat(configuration.socketTimeout()).isEqualTo(500);
      assertThat(configuration.connectionTimeout()).isEqualTo(200);
      assertThat(configuration.maxRetries()).isEqualTo(30);
      assertThat(configuration.batchSize()).isEqualTo(91);
      assertThat(configuration.version()).isEqualTo(ProtocolVersion.PROTOCOL_VERSION_30);

      // Thread pool properties
      assertThat(configuration.asyncExecutorFactory().factory()).isInstanceOf(DefaultAsyncExecutorFactory.class);
      // TODO: how to assert thread pool size ? default-executor-factory-pool-size

      // Marshalling properties
      assertThat(configuration.marshallerClass()).isEqualTo(JavaSerializationMarshaller.class);
      assertThat(configuration.forceReturnValues()).isTrue();
      assertThat(configuration.serialAllowList()).contains("APP-KILLER1", "APP-KILLER2");
      // TODO: Consistent Hash Impl ??
      //assertThat(configuration.consistentHashImpl().getClass().toString()).isEqualTo("");

      // Encryption properties
      assertThat(configuration.security().ssl().enabled()).isTrue();
      assertThat(configuration.security().ssl().keyStoreFileName()).isEqualTo("classpath:superKeyStoreFile.pfx");
      assertThat(configuration.security().ssl().keyStoreType()).isEqualTo("PKCS12");
      assertThat(configuration.security().ssl().keyStorePassword().length).isEqualTo(6);
      assertThat(configuration.security().ssl().keyAlias()).isEqualTo("superAliasKey");
      assertThat(configuration.security().ssl().trustStoreFileName()).isEqualTo("classpath:superTrustFile.pfx");
      assertThat(configuration.security().ssl().trustStorePath()).isNull();
      assertThat(configuration.security().ssl().trustStoreType()).isEqualTo("PKCS12");
      assertThat(configuration.security().ssl().trustStorePassword().length).isEqualTo(6);
      assertThat(configuration.security().ssl().sniHostName()).isEqualTo("elahost");
      assertThat(configuration.security().ssl().protocol()).isEqualTo("TLSv1.3");

      // authentication
      assertThat(configuration.security().authentication().enabled()).isTrue();
      assertThat(configuration.security().authentication().callbackHandler().getClass()).isEqualTo(BasicCallbackHandler.class);
      assertThat(configuration.security().authentication().saslMechanism()).isEqualTo("my-sasl-mechanism");
      assertThat(configuration.security().authentication().serverName()).isEqualTo("my-server-name");
      BasicCallbackHandler basicCallbackHandler = (BasicCallbackHandler) configuration.security().authentication().callbackHandler();
      NameCallback nameCallback = new NameCallback("test", "test");
      PasswordCallback passwordCallback = new PasswordCallback("test", false);
      RealmCallback realmCallback = new RealmCallback("test", "test");
      basicCallbackHandler.handle(new Callback[]{nameCallback, passwordCallback, realmCallback});
      assertThat(nameCallback.getName()).isEqualTo("oihana");
      assertThat(passwordCallback.getPassword()).isEqualTo("oipass".toCharArray());
      assertThat(realmCallback.getText()).isEqualTo("oirealm");
      assertThat(configuration.security().authentication().saslProperties()).hasSize(2);
      assertThat(configuration.security().authentication().saslProperties()).containsKeys("prop1", "prop2");
      assertThat(configuration.security().authentication().saslProperties()).containsValues("value1", "value2");

      // transactions
      assertThat(configuration.transactionTimeout()).isEqualTo(50000);

      // xsite
      assertThat(configuration.clusters()).hasSize(1);
      ClusterConfiguration site = configuration.clusters().get(0);
      assertThat(site.getCluster()).extracting("host", "port").containsExactly(tuple("hostOi1", 21222), tuple("hostOi2", 21223));

      // statistics
      assertThat(configuration.statistics().enabled()).isTrue();
      assertThat(configuration.statistics().jmxEnabled()).isTrue();
      assertThat(configuration.statistics().jmxName()).isEqualTo("oiJmx");
      assertThat(configuration.statistics().jmxDomain()).isEqualTo("oiJmxDom");

      // custom caches from properties
      assertThat(configuration.remoteCaches()).hasSize(4);
      RemoteCacheConfiguration example = configuration.remoteCaches().get("example");
      assertThat(example).isNotNull();
      assertThat(example.forceReturnValues()).isTrue();
      RemoteCacheConfiguration mycache = configuration.remoteCaches().get("mycache");
      assertThat(mycache).isNotNull();
      assertThat(mycache.forceReturnValues()).isFalse();
      assertThat(mycache.templateName()).isEqualTo("myTemplate");
      assertThat(mycache.marshallerClass()).isEqualTo(JavaSerializationMarshaller.class);
      assertThat(mycache.nearCacheMode()).isEqualTo(NearCacheMode.DISABLED);
      assertThat(mycache.transactionMode()).isEqualTo(TransactionMode.NONE);
      RemoteCacheConfiguration yourCache = configuration.remoteCaches().get("org.infinispan.yourcache");
      assertThat(yourCache).isNotNull();
      assertThat(yourCache.templateName()).isEqualTo("org.infinispan.DIST_ASYNC");
      assertThat(yourCache.marshallerClass()).isEqualTo(UTF8StringMarshaller.class);
      assertThat(yourCache.nearCacheMode()).isEqualTo(NearCacheMode.INVALIDATED);
      RemoteCacheConfiguration starredCache = configuration.remoteCaches().get("org.infinispan.*");
      assertThat(starredCache).isNotNull();
      assertThat(starredCache.templateName()).isEqualTo("org.infinispan.REPL_SYNC");
      assertThat(starredCache.transactionMode()).isEqualTo(TransactionMode.NON_XA);
   }

}
