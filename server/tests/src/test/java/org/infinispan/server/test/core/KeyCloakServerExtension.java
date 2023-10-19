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
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.test.TestingUtil;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class KeyCloakServerExtension implements AfterAllCallback, BeforeAllCallback {
   public static final String KEYCLOAK_IMAGE = System.getProperty(TestSystemPropertyNames.KEYCLOAK_IMAGE, "quay.io/keycloak/keycloak:latest");
   private final String realmJsonFile;

   private FixedHostPortGenericContainer<?> container;
   private final List<Consumer<KeyCloakServerExtension>> beforeListeners = new ArrayList<>();
   private final File keycloakDirectory;

   public KeyCloakServerExtension(String realmJsonFile) {
      this.realmJsonFile = realmJsonFile;
      this.keycloakDirectory = new File(tmpDirectory("keycloak"));
   }

   @Override
   public void beforeAll(ExtensionContext context) {
      Class<?> testClass = context.getRequiredTestClass();
      keycloakDirectory.mkdirs();

      File keycloakImport = new File(keycloakDirectory, "keycloak.json");
      try (InputStream is = getClass().getClassLoader().getResourceAsStream(realmJsonFile); OutputStream os = new FileOutputStream(keycloakImport)) {
         TestingUtil.copy(is, os);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      final Map<String, String> environment = new HashMap<>();
      environment.put(System.getProperty(TestSystemPropertyNames.KEYCLOAK_USER, "KEYCLOAK_ADMIN"), "keycloak");
      environment.put(System.getProperty(TestSystemPropertyNames.KEYCLOAK_PASSWORD, "KEYCLOAK_ADMIN_PASSWORD"), "keycloak");
      container = new FixedHostPortGenericContainer<>(KEYCLOAK_IMAGE);
      container
            .withFixedExposedPort(14567, 8080)
            .withFixedExposedPort(14568, 8443)
            .withEnv(environment)
            .withCopyFileToContainer(MountableFile.forHostPath(keycloakImport.getAbsolutePath()), "/opt/keycloak/data/import/infinispan-realm.json")
            .withCommand("start-dev", "--import-realm")
            .waitingFor(Wait.forLogMessage(".*org.keycloak.quarkus.runtime.KeycloakMain.*",1))
            .withLogConsumer(new JBossLoggingConsumer(LogFactory.getLog(testClass)));
      beforeListeners.forEach(l -> l.accept(this));
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
      builder.addServer().host(container.getHost()).port(container.getMappedPort(port)).connectionTimeout(5000).socketTimeout(5000);
      try (RestClient c = RestClient.forConfiguration(builder.build())) {
         String url = String.format("/realms/%s/protocol/openid-connect/token", realm);
         Map<String, List<String>> form = new HashMap<>();
         form.put("client_id", Collections.singletonList(client));
         form.put("client_secret", Collections.singletonList(secret));
         form.put("username", Collections.singletonList(username));
         form.put("password", Collections.singletonList(password));
         form.put("grant_type", Collections.singletonList("password"));
         RestResponse response = c.raw().post(url, RestEntity.form(form)).toCompletableFuture().get(5, TimeUnit.SECONDS);
         Map<String, Json> map = Json.read(response.body()).asJsonMap();
         return map.get("access_token").asString();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void afterAll(ExtensionContext context) {
      container.close();
   }

   public KeyCloakServerExtension addBeforeListener(Consumer<KeyCloakServerExtension> listener) {
      beforeListeners.add(listener);
      return this;
   }

   public GenericContainer<?> getKeycloakContainer() {
      return container;
   }
}
