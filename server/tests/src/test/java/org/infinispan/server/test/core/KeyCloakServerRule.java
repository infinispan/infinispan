package org.infinispan.server.test.core;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.test.TestingUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class KeyCloakServerRule implements TestRule {
   public static String KEYCLOAK_IMAGE;
   private final String realmJsonFile;
   private final String realmSsoFile;

   private FixedHostPortGenericContainer container;

   public KeyCloakServerRule(String realmJsonFile,String realmSsoFile,String keycloak_image) {
      this.realmJsonFile = realmJsonFile;
      this.realmSsoFile=realmSsoFile;
      this.KEYCLOAK_IMAGE=keycloak_image;
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
      File keycloakDirectory = new File(tmpDirectory("keycloak"));
      keycloakDirectory.mkdirs();
      File keycloakImport = new File(keycloakDirectory, "keycloak.json");
      File ssoImport = new File(keycloakDirectory, "sso-realm.sh");
      try (InputStream is = getClass().getClassLoader().getResourceAsStream(realmJsonFile); InputStream is_sso = getClass().getClassLoader().getResourceAsStream(realmSsoFile); OutputStream os = new FileOutputStream(keycloakImport); OutputStream os_sso = new FileOutputStream(ssoImport)) {
         TestingUtil.copy(is, os);
         TestingUtil.copy(is_sso, os_sso);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      final Map<String, String> environment = new HashMap<>();
      environment.put("DB_VENDOR", "h2");
      environment.put("SSO_ADMIN_USERNAME", "keycloak");
      environment.put("SSO_ADMIN_PASSWORD", "keycloak");
      environment.put("KEYCLOAK_IMPORT", keycloakImport.getAbsolutePath());
      environment.put("SSO_IMPORT", ssoImport.getAbsolutePath());
      container = new FixedHostPortGenericContainer(KEYCLOAK_IMAGE);
      container.withFixedExposedPort(14567, 8080)
            .withEnv(environment)
            .withCopyFileToContainer(MountableFile.forHostPath(keycloakImport.getAbsolutePath()), keycloakImport.getPath())
            .withCopyFileToContainer(MountableFile.forHostPath(ssoImport.getAbsolutePath(),700), ssoImport.getPath())
            .withLogConsumer(new JBossLoggingConsumer(LogFactory.getLog(testClass)))
            .waitingFor(Wait.forHttp("/"));
      container.start();
      try {
         container.execInContainer(ssoImport.getPath());
      } catch (IOException e) {
         throw new RuntimeException(e);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
   }

   public String getAccessTokenForCredentials(String realm, String client, String secret, String username, String password) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(container.getContainerIpAddress()).port(container.getMappedPort(8080)).connectionTimeout(5000).socketTimeout(5000);
      try (RestClient c = RestClient.forConfiguration(builder.build())) {
         String url = String.format("/auth/realms/%s/protocol/openid-connect/token", realm);
         Map<String, String> form = new HashMap<>();
         form.put("client_id", client);
         form.put("client_secret", secret);
         form.put("username", username);
         form.put("password", password);
         form.put("grant_type", "password");
         RestResponse response = c.raw().postForm(url, Collections.singletonMap("Content-Type", "application/x-www-form-urlencoded"), form).toCompletableFuture().get(5, TimeUnit.SECONDS);
         ObjectMapper mapper = new ObjectMapper();
         Map<String, String> map = mapper.readValue(response.getBody(), Map.class);
         return map.get("access_token");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private void after() {
      container.close();
   }
}
