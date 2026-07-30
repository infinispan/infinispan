package org.infinispan.cli.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Properties;

import org.aesh.command.CommandResult;
import org.infinispan.cli.Context;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.WildFlyElytronCredentialStoreProvider;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.WildFlyElytronPasswordProvider;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @since 16.0
 */
@Cli
public class BookmarkTest {

   private static final String MASTER_PASSWORD = "testMasterPassword";

   @RegisterExtension
   CliExtension cli = new CliExtension(MASTER_PASSWORD);

   @BeforeAll
   public static void registerProviders() {
      java.security.Security.addProvider(WildFlyElytronCredentialStoreProvider.getInstance());
      java.security.Security.addProvider(WildFlyElytronPasswordProvider.getInstance());
   }

   @Test
   public void testSetAndGetBookmark() {
      assertEquals(0, cli.run("bookmark", "set", "myserver", "-u", "hotrod://localhost:11222"));
      assertTrue(cli.shell().getBuffer().contains("Bookmark 'myserver' saved"));

      assertEquals(0, cli.run("bookmark", "get", "myserver"));
      assertTrue(cli.shell().getBuffer().contains("url = hotrod://localhost:11222"));
   }

   @Test
   public void testUpdateBookmark() {
      assertEquals(0, cli.run("bookmark", "set", "myserver", "-u", "hotrod://old:11222"));

      assertEquals(0, cli.run("bookmark", "set", "myserver", "-u", "hotrod://new:11222", "--username", "user1"));

      assertEquals(0, cli.run("bookmark", "get", "myserver"));
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("url = hotrod://new:11222"));
      assertTrue(output.contains("username = user1"));
   }

   @Test
   public void testListBookmarks() {
      assertEquals(0, cli.run("bookmark", "set", "alpha", "-u", "hotrod://alpha:11222"));
      assertEquals(0, cli.run("bookmark", "set", "beta", "-u", "https://beta:11222", "--username", "admin"));

      assertEquals(0, cli.run("bookmark", "ls"));
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("alpha = hotrod://alpha:11222"));
      assertTrue(output.contains("beta = https://beta:11222 (user: admin)"));
   }

   @Test
   public void testListEmpty() {
      assertEquals(0, cli.run("bookmark", "ls"));
      assertTrue(cli.shell().getBuffer().contains("No bookmarks defined"));
   }

   @Test
   public void testGetNonExistent() {
      int rc = cli.run("bookmark", "get", "nonexistent");
      assertTrue(cli.shell().getBuffer().contains("Bookmark 'nonexistent' not found"));
   }

   @Test
   public void testSetStripsUsernameOnlyFromUrl() throws IOException {
      assertEquals(0, cli.run("bookmark", "set", "useronly", "-u", "hotrod://admin@localhost:11222"));

      Properties props = loadProps();
      assertEquals("hotrod://localhost:11222", props.getProperty("useronly.url"));
      assertEquals("admin", props.getProperty("useronly.username"));
   }

   @Test
   public void testSetRequiresUrlWhenDisconnected() {
      int rc = cli.run("bookmark", "set", "nourl");
      assertTrue(rc != 0 || cli.shell().getBuffer().toLowerCase().contains("url"), cli.shell().getBuffer());
   }

   // --- Tests below require interactive master password or custom connections ---
   // They use the direct Java API and only run in embedded mode.

   @Test
   public void testSetWithAllOptions() throws Exception {
      if (cli.isProcess()) return;
      runSet("full", "https://server:11222", "admin", "secret", "/path/ts", "tspass", "/path/ks", "kspass");

      runGet("full");
      String output = cli.shell().getBuffer();
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
      if (cli.isProcess()) return;
      runSet("secure", "hotrod://host:11222", "admin", "secret", null, null, null, null);

      Properties props = loadProps();
      assertEquals("hotrod://host:11222", props.getProperty("secure.url"));
      assertEquals("admin", props.getProperty("secure.username"));
      assertNull(props.getProperty("secure.password"));
   }

   @Test
   public void testSecretsInCredentialStore() throws Exception {
      if (cli.isProcess()) return;
      runSet("secure", "hotrod://host:11222", null, "mypass", null, "tspass", null, "kspass");

      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(
            cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());

      assertCredential(store, "bookmark.secure.password", "mypass");
      assertCredential(store, "bookmark.secure.truststore-password", "tspass");
      assertCredential(store, "bookmark.secure.keystore-password", "kspass");
   }

   @Test
   public void testRemoveBookmark() throws Exception {
      if (cli.isProcess()) return;
      runSet("toremove", "hotrod://host:11222", "user", "pass", null, null, null, null);
      cli.shell().clear();

      CommandResult result = runRemove("toremove");
      assertEquals(CommandResult.SUCCESS, result);
      assertTrue(cli.shell().getBuffer().contains("Bookmark 'toremove' removed"));

      cli.shell().clear();
      result = runGet("toremove");
      assertEquals(CommandResult.FAILURE, result);

      Properties props = loadProps();
      for (String key : props.stringPropertyNames()) {
         assertFalse(key.startsWith("toremove."), "Key should not start with toremove.: " + key);
      }
   }

   @Test
   public void testRemoveAlsoRemovesCredentials() throws Exception {
      if (cli.isProcess()) return;
      runSet("removeme", "hotrod://host:11222", null, "secret", null, null, null, null);

      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(
            cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertTrue(store.exists("bookmark.removeme.password", PasswordCredential.class));

      cli.shell().clear();
      runRemove("removeme");

      store = Credentials.getKeyStoreCredentialStore(
            cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertFalse(store.exists("bookmark.removeme.password", PasswordCredential.class));
   }

   @Test
   public void testRemoveNonExistent() throws Exception {
      if (cli.isProcess()) return;
      CommandResult result = runRemove("nonexistent");
      assertEquals(CommandResult.FAILURE, result);
      assertTrue(cli.shell().getBuffer().contains("Bookmark 'nonexistent' not found"));
   }

   @Test
   public void testSetEmptyStringsTreatedAsUnset() throws Exception {
      if (cli.isProcess()) return;
      CliExtension.StubConnection connection = new CliExtension.StubConnection("https://inferred-host:11222", "inferreduser");
      Properties props = new Properties();
      ContextAwareCommandInvocation invocation = cli.invocation(connection, props);

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
      cmd.execute(invocation);

      cli.shell().clear();
      runGet("emptytest");
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("url = https://inferred-host:11222"), output);
      assertTrue(output.contains("username = inferreduser"), output);
   }

   @Test
   public void testSetInfersFromConnection() throws Exception {
      if (cli.isProcess()) return;
      CliExtension.StubConnection connection = new CliExtension.StubConnection("https://prod-server:11222", "admin");
      Properties contextProps = new Properties();
      contextProps.setProperty(Context.Property.TRUSTSTORE.propertyName(), "/path/to/truststore.pfx");
      contextProps.setProperty(Context.Property.TRUSTSTORE_PASSWORD.propertyName(), "tspass");
      ContextAwareCommandInvocation invocation = cli.invocation(connection, contextProps);

      Bookmark.SetBookmark cmd = new Bookmark.SetBookmark();
      cmd.name = "inferred";
      cmd.execute(invocation);

      cli.shell().clear();
      runGet("inferred");
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("url = https://prod-server:11222"));
      assertTrue(output.contains("username = admin"));
      assertTrue(output.contains("truststore = /path/to/truststore.pfx"));
   }

   @Test
   public void testSetExplicitOverridesConnection() throws Exception {
      if (cli.isProcess()) return;
      CliExtension.StubConnection connection = new CliExtension.StubConnection("https://prod-server:11222", "admin");
      ContextAwareCommandInvocation invocation = cli.invocation(connection, new Properties());

      Bookmark.SetBookmark cmd = new Bookmark.SetBookmark();
      cmd.name = "override";
      cmd.url = "hotrod://other:11222";
      cmd.username = "otheruser";
      cmd.execute(invocation);

      cli.shell().clear();
      runGet("override");
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("url = hotrod://other:11222"));
      assertTrue(output.contains("username = otheruser"));
   }

   @Test
   public void testSetStripsCredentialsFromUrl() throws Exception {
      if (cli.isProcess()) return;
      runSet("withcreds", "https://admin:secret@server:11222", null, null, null, null, null, null);

      Properties props = loadProps();
      assertEquals("https://server:11222", props.getProperty("withcreds.url"));
      assertEquals("admin", props.getProperty("withcreds.username"));
      assertNull(props.getProperty("withcreds.password"));

      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(
            cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertCredential(store, "bookmark.withcreds.password", "secret");
   }

   @Test
   public void testSetExplicitCredsOverrideUrlCreds() throws Exception {
      if (cli.isProcess()) return;
      runSet("explicit", "https://urluser:urlpass@server:11222", "myuser", "mypass", null, null, null, null);

      Properties props = loadProps();
      assertEquals("https://server:11222", props.getProperty("explicit.url"));
      assertEquals("myuser", props.getProperty("explicit.username"));

      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(
            cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertCredential(store, "bookmark.explicit.password", "mypass");
   }

   @Test
   public void testCredentialStoreFilePermissions() throws Exception {
      if (cli.isProcess()) return;
      runSet("permtest", "hotrod://host:11222", null, "secret", null, null, null, null);

      Path storePath = cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE);
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
      cmd.execute(cli.invocation());
   }

   private CommandResult runGet(String name) throws Exception {
      Bookmark.Get cmd = new Bookmark.Get();
      cmd.name = name;
      return cmd.execute(cli.invocation());
   }

   private CommandResult runRemove(String name) throws Exception {
      Bookmark.Remove cmd = new Bookmark.Remove();
      cmd.name = name;
      return cmd.execute(cli.invocation());
   }

   private Properties loadProps() throws IOException {
      Path bookmarksFile = cli.configPath().resolve(Bookmark.BOOKMARKS_FILE);
      Properties props = new Properties();
      if (Files.exists(bookmarksFile)) {
         try (Reader r = Files.newBufferedReader(bookmarksFile)) {
            props.load(r);
         }
      }
      return props;
   }
}
