package org.infinispan.cli.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.Executor;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.invocation.CommandInvocationConfiguration;
import org.aesh.command.registry.CommandRegistry;
import org.aesh.command.shell.Shell;
import org.aesh.console.ReadlineConsole;
import org.aesh.readline.Prompt;
import org.aesh.terminal.Key;
import org.aesh.terminal.KeyAction;
import org.aesh.terminal.tty.Size;
import org.aesh.terminal.utils.Config;
import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.SSLContextSettings;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.WildFlyElytronCredentialStoreProvider;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.WildFlyElytronPasswordProvider;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @since 16.0
 */
public class BookmarkTest {

   private static final String MASTER_PASSWORD = "testMasterPassword";

   @BeforeClass
   public static void registerProviders() {
      java.security.Security.addProvider(WildFlyElytronCredentialStoreProvider.getInstance());
      java.security.Security.addProvider(WildFlyElytronPasswordProvider.getInstance());
   }

   @Rule
   public TemporaryFolder tempFolder = new TemporaryFolder();

   private Path configPath;
   private TestShell shell;

   @Before
   public void setup() throws IOException {
      configPath = tempFolder.newFolder("config").toPath();
      shell = new TestShell();
   }

   @Test
   public void testSetAndGetBookmark() throws Exception {
      runSet("myserver", "hotrod://localhost:11222", null, null, null, null, null, null);
      assertTrue(shell.getBuffer().contains("Bookmark 'myserver' saved"));

      shell.clear();
      runGet("myserver");
      assertTrue(shell.getBuffer().contains("url = hotrod://localhost:11222"));
   }

   @Test
   public void testSetWithAllOptions() throws Exception {
      runSet("full", "https://server:11222", "admin", "secret", "/path/ts", "tspass", "/path/ks", "kspass");
      shell.clear();

      runGet("full");
      String output = shell.getBuffer();
      assertTrue(output.contains("url = https://server:11222"));
      assertTrue(output.contains("username = admin"));
      assertTrue(output.contains("password = ********"));
      assertTrue(output.contains("truststore = /path/ts"));
      assertTrue(output.contains("truststore-password = ********"));
      assertTrue(output.contains("keystore = /path/ks"));
      assertTrue(output.contains("keystore-password = ********"));
   }

   @Test
   public void testSecretsNotInPropertiesFile() throws Exception {
      runSet("secure", "hotrod://host:11222", "admin", "secret", null, null, null, null);

      Properties props = loadProps();
      // URL and username are in properties
      assertEquals("hotrod://host:11222", props.getProperty("secure.url"));
      assertEquals("admin", props.getProperty("secure.username"));
      // Password must NOT be in properties file
      assertNull(props.getProperty("secure.password"));
   }

   @Test
   public void testSecretsInCredentialStore() throws Exception {
      runSet("secure", "hotrod://host:11222", null, "mypass", null, "tspass", null, "kspass");

      // Verify secrets are in credential store
      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(
            configPath.resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());

      assertCredential(store, "bookmark.secure.password", "mypass");
      assertCredential(store, "bookmark.secure.truststore-password", "tspass");
      assertCredential(store, "bookmark.secure.keystore-password", "kspass");
   }

   @Test
   public void testUpdateBookmark() throws Exception {
      runSet("myserver", "hotrod://old:11222", null, null, null, null, null, null);
      shell.clear();

      runSet("myserver", "hotrod://new:11222", "user1", null, null, null, null, null);
      shell.clear();

      runGet("myserver");
      String output = shell.getBuffer();
      assertTrue(output.contains("url = hotrod://new:11222"));
      assertTrue(output.contains("username = user1"));
   }

   @Test
   public void testListBookmarks() throws Exception {
      runSet("alpha", "hotrod://alpha:11222", null, null, null, null, null, null);
      shell.clear();
      runSet("beta", "https://beta:11222", "admin", null, null, null, null, null);
      shell.clear();

      runLs();
      String output = shell.getBuffer();
      assertTrue(output.contains("alpha = hotrod://alpha:11222"));
      assertTrue(output.contains("beta = https://beta:11222 (user: admin)"));
   }

   @Test
   public void testListEmpty() throws Exception {
      runLs();
      assertTrue(shell.getBuffer().contains("No bookmarks defined"));
   }

   @Test
   public void testRemoveBookmark() throws Exception {
      runSet("toremove", "hotrod://host:11222", "user", "pass", null, null, null, null);
      shell.clear();

      CommandResult result = runRemove("toremove");
      assertEquals(CommandResult.SUCCESS, result);
      assertTrue(shell.getBuffer().contains("Bookmark 'toremove' removed"));

      shell.clear();
      result = runGet("toremove");
      assertEquals(CommandResult.FAILURE, result);

      // Verify properties file has no leftover keys
      Properties props = loadProps();
      for (String key : props.stringPropertyNames()) {
         assertFalse("Key should not start with toremove.: " + key, key.startsWith("toremove."));
      }
   }

   @Test
   public void testRemoveAlsoRemovesCredentials() throws Exception {
      runSet("removeme", "hotrod://host:11222", null, "secret", null, null, null, null);

      // Verify credential exists
      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(
            configPath.resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertTrue(store.exists("bookmark.removeme.password", PasswordCredential.class));

      shell.clear();
      runRemove("removeme");

      // Verify credential was removed
      store = Credentials.getKeyStoreCredentialStore(
            configPath.resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertFalse(store.exists("bookmark.removeme.password", PasswordCredential.class));
   }

   @Test
   public void testRemoveNonExistent() throws Exception {
      CommandResult result = runRemove("nonexistent");
      assertEquals(CommandResult.FAILURE, result);
      assertTrue(shell.getBuffer().contains("Bookmark 'nonexistent' not found"));
   }

   @Test
   public void testGetNonExistent() throws Exception {
      CommandResult result = runGet("nonexistent");
      assertEquals(CommandResult.FAILURE, result);
      assertTrue(shell.getBuffer().contains("Bookmark 'nonexistent' not found"));
   }

   @Test
   public void testSetEmptyStringsTreatedAsUnset() throws Exception {
      // Aesh sets unspecified options to empty string, not null
      StubConnection connection = new StubConnection("https://inferred-host:11222", "inferreduser");
      StubContext context = new StubContext(configPath, connection, new Properties());

      Bookmark.SetBookmark cmd = new Bookmark.SetBookmark();
      cmd.name = "emptytest";
      cmd.url = "";
      cmd.username = "";
      cmd.password = "";
      cmd.truststore = "";
      cmd.truststorePassword = "";
      cmd.keystore = "";
      cmd.keystorePassword = "";
      cmd.hostnameVerifier = "";
      cmd.execute(new ContextAwareCommandInvocation(new StubCommandInvocation(shell), context));

      shell.clear();
      runGet("emptytest");
      String output = shell.getBuffer();
      // Empty strings should be treated as unset, so connection values should be inferred
      assertTrue(output, output.contains("url = https://inferred-host:11222"));
      assertTrue(output, output.contains("username = inferreduser"));
   }

   @Test
   public void testSetInfersFromConnection() throws Exception {
      StubConnection connection = new StubConnection("https://prod-server:11222", "admin");
      Properties contextProps = new Properties();
      contextProps.setProperty(Context.Property.TRUSTSTORE.propertyName(), "/path/to/truststore.pfx");
      contextProps.setProperty(Context.Property.TRUSTSTORE_PASSWORD.propertyName(), "tspass");
      StubContext context = new StubContext(configPath, connection, contextProps);

      Bookmark.SetBookmark cmd = new Bookmark.SetBookmark();
      cmd.name = "inferred";
      cmd.execute(new ContextAwareCommandInvocation(new StubCommandInvocation(shell), context));

      shell.clear();
      runGet("inferred");
      String output = shell.getBuffer();
      assertTrue(output.contains("url = https://prod-server:11222"));
      assertTrue(output.contains("username = admin"));
      assertTrue(output.contains("truststore = /path/to/truststore.pfx"));
   }

   @Test
   public void testSetExplicitOverridesConnection() throws Exception {
      StubConnection connection = new StubConnection("https://prod-server:11222", "admin");
      StubContext context = new StubContext(configPath, connection, new Properties());

      Bookmark.SetBookmark cmd = new Bookmark.SetBookmark();
      cmd.name = "override";
      cmd.url = "hotrod://other:11222";
      cmd.username = "otheruser";
      cmd.execute(new ContextAwareCommandInvocation(new StubCommandInvocation(shell), context));

      shell.clear();
      runGet("override");
      String output = shell.getBuffer();
      assertTrue(output.contains("url = hotrod://other:11222"));
      assertTrue(output.contains("username = otheruser"));
   }

   @Test
   public void testSetRequiresUrlWhenDisconnected() throws Exception {
      Bookmark.SetBookmark cmd = new Bookmark.SetBookmark();
      cmd.name = "nourl";
      CommandResult result = cmd.execute(createInvocation());
      assertEquals(CommandResult.FAILURE, result);
   }

   @Test
   public void testSetStripsCredentialsFromUrl() throws Exception {
      runSet("withcreds", "https://admin:secret@server:11222", null, null, null, null, null, null);

      // URL in properties should not contain credentials
      Properties props = loadProps();
      assertEquals("https://server:11222", props.getProperty("withcreds.url"));
      assertEquals("admin", props.getProperty("withcreds.username"));
      assertNull(props.getProperty("withcreds.password"));

      // Password should be in credential store
      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(
            configPath.resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertCredential(store, "bookmark.withcreds.password", "secret");
   }

   @Test
   public void testSetStripsUsernameOnlyFromUrl() throws Exception {
      runSet("useronly", "hotrod://admin@localhost:11222", null, null, null, null, null, null);

      Properties props = loadProps();
      assertEquals("hotrod://localhost:11222", props.getProperty("useronly.url"));
      assertEquals("admin", props.getProperty("useronly.username"));
   }

   @Test
   public void testSetExplicitCredsOverrideUrlCreds() throws Exception {
      runSet("explicit", "https://urluser:urlpass@server:11222", "myuser", "mypass", null, null, null, null);

      Properties props = loadProps();
      assertEquals("https://server:11222", props.getProperty("explicit.url"));
      // Explicit options take precedence
      assertEquals("myuser", props.getProperty("explicit.username"));

      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(
            configPath.resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertCredential(store, "bookmark.explicit.password", "mypass");
   }

   @Test
   public void testCredentialStoreFilePermissions() throws Exception {
      runSet("permtest", "hotrod://host:11222", null, "secret", null, null, null, null);

      Path storePath = configPath.resolve(Bookmark.CREDENTIAL_STORE_FILE);
      assertTrue(Files.exists(storePath));
      java.util.Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(storePath);
      assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
      assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));
      assertFalse(permissions.contains(PosixFilePermission.GROUP_READ));
      assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE));
      assertFalse(permissions.contains(PosixFilePermission.OTHERS_READ));
      assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE));
   }

   private void assertCredential(KeyStoreCredentialStore store, String alias, String expected) throws Exception {
      PasswordCredential credential = store.retrieve(alias, PasswordCredential.class, null, null, null);
      assertNotNull(credential);
      char[] password = credential.getPassword().castAndApply(ClearPassword.class, ClearPassword::getPassword);
      assertEquals(expected, new String(password));
   }

   private void runSet(String name, String url, String username, String password,
                       String truststore, String truststorePassword,
                       String keystore, String keystorePassword) throws Exception {
      Bookmark.SetBookmark cmd = new Bookmark.SetBookmark();
      cmd.name = name;
      cmd.url = url;
      cmd.username = username;
      cmd.password = password;
      cmd.truststore = truststore;
      cmd.truststorePassword = truststorePassword;
      cmd.keystore = keystore;
      cmd.keystorePassword = keystorePassword;
      cmd.execute(createInvocation());
   }

   private CommandResult runGet(String name) throws Exception {
      Bookmark.Get cmd = new Bookmark.Get();
      cmd.name = name;
      return cmd.execute(createInvocation());
   }

   private void runLs() throws Exception {
      Bookmark.Ls cmd = new Bookmark.Ls();
      cmd.execute(createInvocation());
   }

   private CommandResult runRemove(String name) throws Exception {
      Bookmark.Remove cmd = new Bookmark.Remove();
      cmd.name = name;
      return cmd.execute(createInvocation());
   }

   private Properties loadProps() throws IOException {
      Path bookmarksFile = configPath.resolve(Bookmark.BOOKMARKS_FILE);
      Properties props = new Properties();
      if (Files.exists(bookmarksFile)) {
         try (Reader r = Files.newBufferedReader(bookmarksFile)) {
            props.load(r);
         }
      }
      return props;
   }

   private ContextAwareCommandInvocation createInvocation() {
      Context context = new StubContext(configPath);
      CommandInvocation delegate = new StubCommandInvocation(shell);
      return new ContextAwareCommandInvocation(delegate, context);
   }

   /**
    * A test shell that returns a fixed master password when readLine is called with a masked prompt.
    */
   private static class TestShell implements Shell {
      private final StringBuilder bufferBuilder = new StringBuilder();

      @Override
      public void write(String msg, boolean paging) {
         bufferBuilder.append(msg);
      }

      @Override
      public void writeln(String msg, boolean paging) {
         bufferBuilder.append(msg).append(Config.getLineSeparator());
      }

      @Override
      public void write(int[] out) {
      }

      @Override
      public void write(char out) {
      }

      @Override
      public String readLine() {
         return MASTER_PASSWORD;
      }

      @Override
      public String readLine(Prompt prompt) {
         return MASTER_PASSWORD;
      }

      @Override
      public Key read() {
         return null;
      }

      @Override
      public Key read(long timeout, TimeUnit unit) {
         return null;
      }

      @Override
      public Key read(Prompt prompt) {
         return null;
      }

      @Override
      public boolean enableAlternateBuffer() {
         return false;
      }

      @Override
      public boolean enableMainBuffer() {
         return false;
      }

      @Override
      public Size size() {
         return null;
      }

      @Override
      public void clear() {
         bufferBuilder.setLength(0);
      }

      public String getBuffer() {
         return bufferBuilder.toString();
      }
   }

   private record StubContext(Path configPath, Connection connection, Properties properties) implements Context {
         StubContext(Path configPath) {
            this(configPath, null, new Properties());
         }

      @Override
         public boolean isConnected() {
            return connection != null;
         }

         @Override
         public void connect(Shell shell, String connectionString) {
         }

         @Override
         public void connect(Shell shell, String connectionString, String username, String password) {
         }

         @Override
         public void disconnect() {
         }

         @Override
         public String getProperty(String key) {
            return properties.getProperty(key);
         }

         @Override
         public String getProperty(Property key) {
            return properties.getProperty(key.propertyName());
         }

         @Override
         public String getProperty(Property property, String defaultValue) {
            return properties.getProperty(property.propertyName(), defaultValue);
         }

         @Override
         public void resetProperties() {
         }

         @Override
         public void setProperty(String key, String value) {
         }

         @Override
         public void saveProperties() {
         }

         @Override
         public void setSslContext(SSLContextSettings sslContext) {
         }

         @Override
         public void setRegistry(CommandRegistry registry) {
         }

         @Override
         public void setConsole(ReadlineConsole console) {
         }

         @Override
         public CommandRegistry getRegistry() {
            return null;
         }

         @Override
         public MediaType getEncoding() {
            return null;
         }

         @Override
         public void setEncoding(MediaType encoding) {
         }

         @Override
         public void refreshPrompt() {
         }

         @Override
         public CommandResult changeResource(Class<? extends Resource> fromResource, String resourceType, String name) throws CommandException {
            return null;
         }

         @Override
         public org.aesh.io.Resource getCurrentWorkingDirectory() {
            return null;
         }

         @Override
         public void setCurrentWorkingDirectory(org.aesh.io.Resource resource) {
         }

         @Override
         public Set<String> exportedVariableNames() {
            return Set.of();
         }

         @Override
         public String exportedVariable(String name) {
            return null;
         }
      }

   private static class StubConnection implements Connection {
      private final String uri;
      private final String username;

      StubConnection(String uri, String username) {
         this.uri = uri;
         this.username = username;
      }

      @Override
      public void connect() {
      }

      @Override
      public void connect(String username, String password) {
      }

      @Override
      public String getURI() {
         return uri;
      }

      @Override
      public String getUsername() {
         return username;
      }

      @Override
      public String execute(BiFunction<RestClient, Resource, CompletionStage<RestResponse>> op, ResponseMode responseMode) {
         return null;
      }

      @Override
      public Resource getActiveResource() {
         return null;
      }

      @Override
      public void setActiveResource(Resource resource) {
      }

      @Override
      public Resource getActiveContainer() {
         return null;
      }

      @Override
      public Collection<String> getAvailableCaches() {
         return null;
      }

      @Override
      public Collection<String> getAvailableContainers() {
         return null;
      }

      @Override
      public Collection<String> getAvailableCounters() {
         return null;
      }

      @Override
      public Collection<String> getAvailableCacheConfigurations() {
         return null;
      }

      @Override
      public Collection<String> getAvailableSchemas() {
         return null;
      }

      @Override
      public Collection<String> getAvailableServers() {
         return null;
      }

      @Override
      public Collection<String> getAvailableSites(String cache) {
         return null;
      }

      @Override
      public Collection<String> getAvailableTasks() {
         return null;
      }

      @Override
      public Iterable<Map<String, String>> getCacheKeys(String cache) {
         return null;
      }

      @Override
      public Iterable<Map<String, String>> getCacheKeys(String cache, int limit) {
         return null;
      }

      @Override
      public Iterable<Map<String, String>> getCacheEntries(String cache, int limit, boolean metadata) {
         return null;
      }

      @Override
      public Iterable<String> getCounterValue(String counter) {
         return null;
      }

      @Override
      public boolean isConnected() {
         return true;
      }

      @Override
      public String describeContainer() {
         return null;
      }

      @Override
      public String describeCache(String cache) {
         return null;
      }

      @Override
      public String describeKey(String cache, String key) {
         return null;
      }

      @Override
      public String describeConfiguration(String configuration) {
         return null;
      }

      @Override
      public String describeCounter(String counter) {
         return null;
      }

      @Override
      public String describeTask(String taskName) {
         return null;
      }

      @Override
      public String getConnectionInfo() {
         return null;
      }

      @Override
      public String getServerVersion() {
         return null;
      }

      @Override
      public Collection<String> getClusterNodes() {
         return null;
      }

      @Override
      public Collection<String> getAvailableLogAppenders() {
         return null;
      }

      @Override
      public Collection<String> getAvailableLoggers() {
         return null;
      }

      @Override
      public Collection<String> getBackupNames() {
         return null;
      }

      @Override
      public Collection<String> getSitesView() {
         return null;
      }

      @Override
      public String getLocalSiteName() {
         return null;
      }

      @Override
      public boolean isRelayNode() {
         return false;
      }

      @Override
      public Collection<String> getRelayNodes() {
         return null;
      }

      @Override
      public Collection<String> getConnectorNames() {
         return null;
      }

      @Override
      public MediaType getEncoding() {
         return null;
      }

      @Override
      public void setEncoding(MediaType encoding) {
      }

      @Override
      public void refreshServerInfo() {
      }

      @Override
      public Collection<String> getDataSourceNames() {
         return null;
      }

      @Override
      public Collection<String> getCacheConfigurationAttributes(String name) {
         return null;
      }

      @Override
      public Collection<String> getRoles() {
         return null;
      }

      @Override
      public void close() {
      }
   }

   private record StubCommandInvocation(Shell shell) implements CommandInvocation {

      @Override
         public Shell getShell() {
            return shell;
         }

         @Override
         public void setPrompt(Prompt prompt) {
         }

         @Override
         public Prompt getPrompt() {
            return null;
         }

         @Override
         public String getHelpInfo(String commandName) {
            return "";
         }

         @Override
         public String getHelpInfo() {
            return "";
         }

         @Override
         public void stop() {
         }

         @Override
         public CommandInvocationConfiguration getConfiguration() {
            return null;
         }

         @Override
         public KeyAction input() {
            return null;
         }

         @Override
         public KeyAction input(long timeout, TimeUnit unit) {
            return null;
         }

         @Override
         public String inputLine() {
            return null;
         }

         @Override
         public String inputLine(Prompt prompt) {
            return null;
         }

         @Override
         public void executeCommand(String input) {
         }

         @Override
         public Executor buildExecutor(String line) {
            return null;
         }

         @Override
         public void print(String msg) {
            shell.write(msg, false);
         }

         @Override
         public void println(String msg) {
            shell.writeln(msg, false);
         }

         @Override
         public void print(String msg, boolean paging) {
            shell.write(msg, paging);
         }

         @Override
         public void println(String msg, boolean paging) {
            shell.writeln(msg, paging);
         }
      }
}
