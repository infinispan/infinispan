package org.infinispan.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Properties;

import org.infinispan.cli.test.CliExtension;
import org.infinispan.cli.test.CliTerminal;
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
   CliExtension cli = new CliExtension(List.of(MASTER_PASSWORD));

   @BeforeAll
   public static void registerProviders() {
      java.security.Security.addProvider(WildFlyElytronCredentialStoreProvider.getInstance());
      java.security.Security.addProvider(WildFlyElytronPasswordProvider.getInstance());
   }

   @Test
   public void testSetAndGetBookmark() {
      CliTerminal terminal = cli.run("bookmark", "set", "myserver", "-u", "hotrod://localhost:11222");
      assertEquals(0, terminal.exitCode());
      terminal.assertContains("Bookmark 'myserver' saved");

      terminal = cli.run("bookmark", "get", "myserver");
      assertEquals(0, terminal.exitCode());
      terminal.assertContains("url = hotrod://localhost:11222");
   }

   @Test
   public void testUpdateBookmark() {
      try (CliTerminal terminal = cli.run("bookmark", "set", "myserver", "-u", "hotrod://old:11222")) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("bookmark", "set", "myserver", "-u", "hotrod://new:11222", "--username", "user1")) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("bookmark", "get", "myserver")) {
         assertEquals(0, terminal.exitCode());
         terminal.assertContains("url = hotrod://new:11222");
         terminal.assertContains("username = user1");
      }
   }

   @Test
   public void testListBookmarks() {
      try (CliTerminal terminal = cli.run("bookmark", "set", "alpha", "-u", "hotrod://alpha:11222")) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("bookmark", "set", "beta", "-u", "https://beta:11222", "--username", "admin")) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("bookmark", "ls")) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("alpha = hotrod://alpha:11222", "beta = https://beta:11222 (user: admin)");
      }
   }

   @Test
   public void testListEmpty() {
      try (CliTerminal terminal = cli.run("bookmark", "ls")) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("No bookmarks defined");
      }
   }

   @Test
   public void testGetNonExistent() {
      try (CliTerminal terminal = cli.run("bookmark", "get", "nonexistent")) {
         assertEquals(1, terminal.exitCode());
         terminal.assertContains("Bookmark 'nonexistent' not found");
      }
   }

   @Test
   public void testSetStripsUsernameOnlyFromUrl() throws IOException {
      try (CliTerminal terminal = cli.run("bookmark", "set", "useronly", "-u", "hotrod://admin@localhost:11222")) {
         assertEquals(0, terminal.exitCode());
      }
      Properties props = loadProps();
      assertEquals("hotrod://localhost:11222", props.getProperty("useronly.url"));
      assertEquals("admin", props.getProperty("useronly.username"));
   }

   @Test
   public void testSetRequiresUrlWhenDisconnected() {
      try (CliTerminal terminal = cli.run("bookmark", "set", "nourl")) {
         assertEquals(1, terminal.exitCode());
         assertThat(terminal.output()).contains("URL is required");
      }
   }

   // --- Tests below require interactive master password or custom connections ---
   // They use the direct Java API and only run in embedded mode.

   @Test
   public void testSetWithAllOptions() throws Exception {
      if (cli.isProcess()) return;
      try (CliTerminal terminal = cli.run("bookmark", "set", "full", "--url=https://server:11222", "--username=admin", "--password=secret", "--truststore=/path/ts", "--truststore-password=tspass", "--keystore=/path/ks", "--keystore-password=kspass")) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("bookmark", "get", "full")) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains(
               "url = https://server:11222",
               "username = admin",
               "password = ********",
               "truststore = /path/ts",
               "truststore-password = ********",
               "keystore = /path/ks",
               "keystore-password = ********"
         );
      }
   }

   @Test
   public void testSecretsNotInPropertiesFile() throws Exception {
      if (cli.isProcess()) return;
      try (CliTerminal terminal = cli.run("bookmark", "set", "secure", "--url=hotrod://host:11222", "--username=admin", "--password=secret")) {
         assertEquals(0, terminal.exitCode());
      }
      Properties props = loadProps();
      assertEquals("hotrod://host:11222", props.getProperty("secure.url"));
      assertEquals("admin", props.getProperty("secure.username"));
      assertNull(props.getProperty("secure.password"));
   }

   @Test
   public void testSecretsInCredentialStore() throws Exception {
      if (cli.isProcess()) return;
      try (CliTerminal terminal = cli.run("bookmark", "set", "secure", "--url=hotrod://host:11222", "--password=mypass", "--truststore-password=tspass", "--keystore-password=kspass")) {
         assertEquals(0, terminal.exitCode());
      }
      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertCredential(store, "bookmark.secure.password", "mypass");
      assertCredential(store, "bookmark.secure.truststore-password", "tspass");
      assertCredential(store, "bookmark.secure.keystore-password", "kspass");
   }

   @Test
   public void testRemoveBookmark() throws Exception {
      if (cli.isProcess()) return;
      try (CliTerminal terminal = cli.run("bookmark", "set", "toremove", "--url=hotrod://host:11222", "--username=user", "--password=pass")) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("bookmark", "remove", "toremove")) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("Bookmark 'toremove' removed");
      }
      try(CliTerminal terminal = cli.run("bookmark", "get", "toremove")) {
         assertEquals(1, terminal.exitCode());
      }

      Properties props = loadProps();
      for (String key : props.stringPropertyNames()) {
         assertFalse(key.startsWith("toremove."), "Key should not start with toremove.: " + key);
      }
   }

   @Test
   public void testRemoveAlsoRemovesCredentials() throws Exception {
      if (cli.isProcess()) return;
      KeyStoreCredentialStore store;
      try (CliTerminal terminal = cli.run("bookmark", "set", "removeme", "--url=hotrod://host:11222", "--password=secret")) {
         assertEquals(0, terminal.exitCode());
         store = Credentials.getKeyStoreCredentialStore(cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
         assertTrue(store.exists("bookmark.removeme.password", PasswordCredential.class));
      }
      try (CliTerminal terminal = cli.run("bookmark", "remove", "removeme")) {
         assertEquals(0, terminal.exitCode());
      }

      store = Credentials.getKeyStoreCredentialStore(cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
      assertFalse(store.exists("bookmark.removeme.password", PasswordCredential.class));
   }

   @Test
   public void testRemoveNonExistent() throws Exception {
      if (cli.isProcess()) return;
      try (CliTerminal terminal = cli.run("bookmark", "remove", "nonexistent")) {
         assertNotEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("Bookmark 'nonexistent' not found");
      }
   }

   @Test
   public void testSetStripsCredentialsFromUrl() throws Exception {
      if (cli.isProcess()) return;
      try (CliTerminal terminal = cli.run("bookmark", "set", "withcreds", "--url=https://admin:secret@server:11222")) {
         assertEquals(0, terminal.exitCode());

         Properties props = loadProps();
         assertEquals("https://server:11222", props.getProperty("withcreds.url"));
         assertEquals("admin", props.getProperty("withcreds.username"));
         assertNull(props.getProperty("withcreds.password"));

         KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
         assertCredential(store, "bookmark.withcreds.password", "secret");
      }
   }

   @Test
   public void testSetExplicitCredsOverrideUrlCreds() throws Exception {
      if (cli.isProcess()) return;
      try (CliTerminal terminal = cli.run("bookmark", "set", "explicit", "--url=https://urluser:urlpass@server:11222", "--username=myuser", "--password=mypass")) {
         assertEquals(0, terminal.exitCode());
         Properties props = loadProps();
         assertThat(props).containsEntry("explicit.url", "https://server:11222");
         assertThat(props).containsEntry("explicit.username", "myuser");

         KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE), Credentials.STORE_TYPE, false, MASTER_PASSWORD.toCharArray());
         assertCredential(store, "bookmark.explicit.password", "mypass");
      }
   }

   @Test
   public void testCredentialStoreFilePermissions() throws Exception {
      if (cli.isProcess()) return;
      try (CliTerminal terminal = cli.run("bookmark", "set", "permtest", "--url=hotrod://host:11222", "--password=secret")) {
         assertEquals(0, terminal.exitCode());
         Path storePath = cli.configPath().resolve(Bookmark.CREDENTIAL_STORE_FILE);
         assertThat(storePath).exists();
         java.util.Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(storePath);
         assertThat(permissions).containsExactlyInAnyOrder(
               PosixFilePermission.OWNER_READ,
               PosixFilePermission.OWNER_WRITE
         );
      }
   }

   private void assertCredential(KeyStoreCredentialStore store, String alias, String expected) throws Exception {
      PasswordCredential credential = store.retrieve(alias, PasswordCredential.class, null, null, null);
      assertNotNull(credential);
      char[] password = credential.getPassword().castAndApply(ClearPassword.class, ClearPassword::getPassword);
      assertEquals(expected, new String(password));
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
