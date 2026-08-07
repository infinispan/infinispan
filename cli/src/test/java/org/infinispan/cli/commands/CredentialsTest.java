package org.infinispan.cli.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;

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

      assertEquals(0, cli.run("credentials", "add", "myalias",
            "--path", storePath.toString(), "-p", STORE_PASSWORD, "-c", "mysecret"));

      int rc = cli.run("credentials", "ls",
            "--path", storePath.toString(), "-p", STORE_PASSWORD);
      assertEquals(0, rc, cli.shell().getBuffer());
      assertTrue(cli.shell().getBuffer().contains("myalias"));
   }

   @Test
   public void testAddAndRemove() throws Exception {
      Path storePath = cli.configPath().resolve("test.pfx");

      assertEquals(0, cli.run("credentials", "add", "toremove",
            "--path", storePath.toString(), "-p", STORE_PASSWORD, "-c", "secret"));

      assertEquals(0, cli.run("credentials", "remove", "toremove",
            "--path", storePath.toString(), "-p", STORE_PASSWORD));

      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(storePath, Credentials.STORE_TYPE, false, STORE_PASSWORD.toCharArray());
      assertFalse(store.exists("toremove", PasswordCredential.class));
   }

   @Test
   public void testAddVerifyCredentialValue() throws Exception {
      Path storePath = cli.configPath().resolve("test.pfx");

      assertEquals(0, cli.run("credentials", "add", "dbpass",
            "--path", storePath.toString(), "-p", STORE_PASSWORD, "-c", "s3cret!"));

      KeyStoreCredentialStore store = Credentials.getKeyStoreCredentialStore(storePath, Credentials.STORE_TYPE, false, STORE_PASSWORD.toCharArray());
      PasswordCredential cred = store.retrieve("dbpass", PasswordCredential.class, null, null, null);
      assertNotNull(cred);
      char[] password = cred.getPassword().castAndApply(ClearPassword.class, ClearPassword::getPassword);
      assertEquals("s3cret!", new String(password));
   }

   @Test
   public void testMask() {
      assertEquals(0, cli.run("credentials", "mask", "changeme", "-s", "abcd1234", "-i", "100"));

      String output = cli.shell().getBuffer();
      assertTrue(output.contains(";abcd1234;100"));
   }

   @Test
   public void testListEmptyStore() {
      Path storePath = cli.configPath().resolve("empty.pfx");

      assertEquals(0, cli.run("credentials", "ls",
            "--path", storePath.toString(), "-p", STORE_PASSWORD));
      assertTrue(cli.shell().getBuffer().isEmpty());
   }

   @Test
   public void testAddMultipleAndList() throws Exception {
      Path storePath = cli.configPath().resolve("test.pfx");

      for (String alias : new String[]{"alpha", "beta", "gamma"}) {
         assertEquals(0, cli.run("credentials", "add", alias,
               "--path", storePath.toString(), "-p", STORE_PASSWORD, "-c", "value-" + alias));
      }

      int rc = cli.run("credentials", "ls",
            "--path", storePath.toString(), "-p", STORE_PASSWORD);
      assertEquals(0, rc, cli.shell().getBuffer());

      String output = cli.shell().getBuffer();
      assertTrue(output.contains("alpha"));
      assertTrue(output.contains("beta"));
      assertTrue(output.contains("gamma"));
   }
}
