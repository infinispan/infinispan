package org.infinispan.server.test.core;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.test.TestingUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class KeyCloakServerRule implements TestRule {
   public static final String KEYCLOAK_IMAGE = System.getProperty(TestSystemPropertyNames.KEYCLOAK_IMAGE, "quay.io/keycloak/keycloak:10.0.1");
   private final String realmJsonFile;

   private FixedHostPortGenericContainer<?> container;
   private final List<Consumer<KeyCloakServerRule>> beforeListeners = new ArrayList<>();
   private final File keycloakDirectory;

   public KeyCloakServerRule(String realmJsonFile) {
      this.realmJsonFile = realmJsonFile;
      this.keycloakDirectory = new File(tmpDirectory("keycloak"));
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            before(description.getTestClass());
            try {
               base.evaluate();
            } finally {
               after();
            }
         }
      };
   }

   private void before(Class<?> testClass) {
      keycloakDirectory.mkdirs();

      beforeListeners.forEach(l -> l.accept(this));

      File keycloakImport = new File(keycloakDirectory, "keycloak.json");
      try (InputStream is = getClass().getClassLoader().getResourceAsStream(realmJsonFile); OutputStream os = new FileOutputStream(keycloakImport)) {
         TestingUtil.copy(is, os);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      final Map<String, String> environment = new HashMap<>();
      environment.put("DB_VENDOR", "h2");
      environment.put(System.getProperty(TestSystemPropertyNames.KEYCLOAK_USER, "KEYCLOAK_USER"), "keycloak");
      environment.put(System.getProperty(TestSystemPropertyNames.KEYCLOAK_PASSWORD, "KEYCLOAK_PASSWORD"), "keycloak");
      environment.put("KEYCLOAK_IMPORT", keycloakImport.getAbsolutePath());
      environment.put("JAVA_OPTS_APPEND", "-Dkeycloak.migration.action=import -Dkeycloak.migration.provider=singleFile -Dkeycloak.migration.file="+keycloakImport.getAbsolutePath());
      container = new FixedHostPortGenericContainer<>(KEYCLOAK_IMAGE);
      container
            .withFixedExposedPort(14567, 8080)
            .withFixedExposedPort(14568, 8443)
            .withEnv(environment)
            .withCopyFileToContainer(MountableFile.forHostPath(keycloakImport.getAbsolutePath()), keycloakImport.getPath())
            .withFileSystemBind(keycloakDirectory.getPath(), "/etc/x509/https")
            //.waitingFor(Wait.forHttp("/").forPort(14567))
            .waitingFor(Wait.forLogMessage(".*WFLYSRV0051.*",1))
            .withLogConsumer(new JBossLoggingConsumer(LogFactory.getLog(testClass)));
      container.start();
   }

   public String getAccessTokenForCredentials(String realm, String client, String secret, String username, String password, Path trustStore, String trustStorePassword) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      int port;
      if (trustStore != null)  {
         builder.security().ssl().trustStoreFileName(trustStore.toString()).trustStorePassword(trustStorePassword.toCharArray()).hostnameVerifier((hostname, session) -> true);
         port = 8443;
      } else {
         port = 8080;
      }
      builder.addServer().host(container.getContainerIpAddress()).port(container.getMappedPort(port)).connectionTimeout(5000).socketTimeout(5000);
      try (RestClient c = RestClient.forConfiguration(builder.build())) {
         String url = String.format("/auth/realms/%s/protocol/openid-connect/token", realm);
         Map<String, List<String>> form = new HashMap<>();
         form.put("client_id", Collections.singletonList(client));
         form.put("client_secret", Collections.singletonList(secret));
         form.put("username", Collections.singletonList(username));
         form.put("password", Collections.singletonList(password));
         form.put("grant_type", Collections.singletonList("password"));
         RestResponse response = c.raw().postForm(url, Collections.singletonMap("Content-Type", "application/x-www-form-urlencoded"), form).toCompletableFuture().get(5, TimeUnit.SECONDS);
         Map<String, Json> map = Json.read(response.getBody()).asJsonMap();
         return map.get("access_token").asString();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private void after() {
      container.close();
   }

   public KeyCloakServerRule addBeforeListener(Consumer<KeyCloakServerRule> listener) {
      beforeListeners.add(listener);
      return this;
   }

   public File getKeycloakDirectory() {
      return keycloakDirectory;
   }
}
