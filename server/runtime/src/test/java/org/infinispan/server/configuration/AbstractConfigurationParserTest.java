package org.infinispan.server.configuration;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.server.Server;
import org.infinispan.server.security.ElytronPasswordProviderSupplier;
import org.infinispan.server.security.KeyStoreUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.wildfly.security.auth.server.IdentityCredentials;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.CredentialStore;
import org.wildfly.security.credential.store.CredentialStoreException;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.interfaces.ClearPassword;

@RunWith(Parameterized.class)
public abstract class AbstractConfigurationParserTest {

   protected final MediaType type;
   public static final char[] PASSWORD = "password".toCharArray();
   public static final char[] SECRET = "secret".toCharArray();

   protected static final String KEYSTORE_FILE_NAME = "ServerConfigurationParserTest-keystore.pfx";
   protected static final String TRUSTSTORE_FILE_NAME = "ServerConfigurationParserTest-truststore.pfx";
   protected static final String CREDENTIALS_FILE_NAME = "ServerConfigurationParserTest-credentials.pfx";

   protected AbstractConfigurationParserTest(MediaType type) {
      this.type = type;
   }

   @BeforeClass
   public static void setup() throws Exception {
      KeyStoreUtils.generateSelfSignedCertificate(pathToKeystore(), null, PASSWORD, PASSWORD, "server", "localhost");
      KeyStoreUtils.generateEmptyKeyStore(getConfigPath().resolve(TRUSTSTORE_FILE_NAME).toString(), SECRET);
      createCredentialStore(getConfigPath().resolve(CREDENTIALS_FILE_NAME), SECRET);
   }

   @Parameterized.Parameters(name = "{0}")
   public static Iterable<MediaType> mediaTypes() {
      return Arrays.asList(MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_YAML);
   }

   protected ServerConfiguration loadAndParseConfiguration() throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      URL url = fileLookup.lookupFileLocation(path(), this.getClass().getClassLoader());
      Properties properties = new Properties();
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, getConfigPath().toString());
      properties.setProperty(Server.INFINISPAN_SERVER_HOME_PATH, Paths.get(System.getProperty("build.directory")).toString());
      ParserRegistry registry = new ParserRegistry(this.getClass().getClassLoader(), false, properties);

      ConfigurationBuilderHolder holder = registry.parse(url);
      GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
      return global.module(ServerConfiguration.class);
   }

   public static Path getConfigPath() {
      return Paths.get(System.getProperty("build.directory"), "test-classes", "configuration");
   }

   protected abstract String path();

   public static String pathToKeystore() {
      Path path = getConfigPath().resolve(KEYSTORE_FILE_NAME);
      return path.toString();
   }

   static void createCredentialStore(Path location, char[] secret) {
      KeyStoreCredentialStore credentialStore = newCredentialStore(location, secret);
      addCredential(credentialStore, "ldap", "strongPassword");
      addCredential(credentialStore, "db", "test");
      addCredential(credentialStore, "keystore", "password");
      addCredential(credentialStore, "oauth2", "1fdca4ec-c416-47e0-867a-3d471af7050f");
      addCredential(credentialStore, "trust", "secret");
   }

   static void addCredential(KeyStoreCredentialStore store, String alias, String credential) {
      try {
         store.store(alias, new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, credential.toCharArray())), null);
         store.flush();
      } catch (CredentialStoreException e) {
         throw new RuntimeException(e);
      }
   }

   static KeyStoreCredentialStore newCredentialStore(Path location, char[] secret) {
      Exceptions.unchecked(() -> {
         Files.deleteIfExists(location);
         Files.createDirectories(location.getParent());
      });
      KeyStoreCredentialStore credentialStore = new KeyStoreCredentialStore();
      final Map<String, String> map = new HashMap<>();
      map.put("location", location.toString());
      map.put("create", "true");
      try {
         credentialStore.initialize(
               map,
               new CredentialStore.CredentialSourceProtectionParameter(
                     IdentityCredentials.NONE.withCredential(new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, secret)))),
               ElytronPasswordProviderSupplier.PROVIDERS
         );
         return credentialStore;
      } catch (CredentialStoreException e) {
         throw new RuntimeException(e);
      }
   }
}
