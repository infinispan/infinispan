package org.infinispan.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;

import org.infinispan.cli.test.CliExtension;
import org.infinispan.cli.test.CliTerminal;
import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @since 16.3
 */
@Cli
public class CredentialsTest {

   private static final String STORE_PASSWORD = "testStorePassword";

   @RegisterExtension
   CliExtension cli = new CliExtension();

   @Test
   public void testAddAndList() throws Exception {
      Path storePath = cli.configPath().resolve("test.pfx");
      CliTerminal terminal = cli.run("credentials", "add", "myalias", "--path", storePath.toString(), "-p", STORE_PASSWORD, "-c", "mysecret");
      assertEquals(0, terminal.exitCode());
      terminal = cli.run("credentials", "ls", "--path", storePath.toString(), "-p", STORE_PASSWORD);
      assertEquals(0, terminal.exitCode());
      terminal.assertContains("myalias");
   }

   @Test
   public void testAddAndRemove() throws Exception {
      Path storePath = cli.configPath().resolve("test.pfx");
      CliTerminal terminal = cli.run("credentials", "add", "toremove", "--path", storePath.toString(), "-p", STORE_PASSWORD, "-c", "secret");
      assertEquals(0, terminal.exitCode());
      terminal = cli.run("credentials", "remove", "toremove", "--path", storePath.toString(), "-p", STORE_PASSWORD);
      assertEquals(0, terminal.exitCode());
      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(storePath, Credentials.STORE_TYPE, false, STORE_PASSWORD.toCharArray());
      assertFalse(store.exists("toremove", PasswordCredential.class));
   }

   @Test
   public void testAddVerifyCredentialValue() throws Exception {
      Path storePath = cli.configPath().resolve("test.pfx");

      CliTerminal terminal = cli.run("credentials", "add", "dbpass", "--path", storePath.toString(), "-p", STORE_PASSWORD, "-c", "s3cret!");
      assertEquals(0, terminal.exitCode());

      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(storePath, Credentials.STORE_TYPE, false, STORE_PASSWORD.toCharArray());
      PasswordCredential cred = store.retrieve("dbpass", PasswordCredential.class, null, null, null);
      assertNotNull(cred);
      char[] password = cred.getPassword().castAndApply(ClearPassword.class, ClearPassword::getPassword);
      assertEquals("s3cret!", new String(password));
   }

   @Test
   public void testMask() {
      CliTerminal terminal = cli.run("credentials", "mask", "changeme", "-s", "abcd1234", "-i", "100");
      assertEquals(0, terminal.exitCode());
      terminal.assertContains(";abcd1234;100");
   }

   @Test
   public void testListEmptyStore() {
      Path storePath = cli.configPath().resolve("empty.pfx");
      CliTerminal terminal = cli.run("credentials", "ls", "--path", storePath.toString(), "-p", STORE_PASSWORD);
      assertEquals(0, terminal.exitCode());
      assertThat(terminal.output()).isEmpty();
   }

   @Test
   public void testAddMultipleAndList() throws Exception {
      Path storePath = cli.configPath().resolve("test.pfx");

      for (String alias : new String[]{"alpha", "beta", "gamma"}) {
         CliTerminal terminal = cli.run("credentials", "add", alias, "--path", storePath.toString(), "-p", STORE_PASSWORD, "-c", "value-" + alias);
         assertEquals(0, terminal.exitCode());
      }

      CliTerminal terminal = cli.run("credentials", "ls", "--path", storePath.toString(), "-p", STORE_PASSWORD);
      assertEquals(0, terminal.exitCode());
      assertThat(terminal.output()).contains("alpha", "beta", "gamma");
   }
}
