package org.infinispan.cli.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 16.3
 */
@Cli
public class UserTest {

   @RegisterExtension
   CliExtension cli = new CliExtension();

   private String serverRoot;
   private Path confDir;

   @BeforeEach
   public void createServerDirs() throws IOException {
      serverRoot = cli.configPath().resolve("server").toString();
      confDir = cli.configPath().resolve("server").resolve("conf");
      Files.createDirectories(confDir);
   }

   @Test
   public void testCreateAndDescribe() throws Exception {
      assertEquals(0, cli.run("user", "create", "admin", "-p", "changeme",
            "--plain-text", "-g", "administrators,deployers", "-s", serverRoot));

      Properties users = loadProperties("users.properties");
      assertEquals("changeme", users.getProperty("admin"));

      assertEquals(0, cli.run("user", "describe", "admin", "-s", serverRoot));
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("admin"));
      assertTrue(output.contains("administrators"));
      assertTrue(output.contains("deployers"));
   }

   @Test
   public void testCreateEncrypted() throws Exception {
      assertEquals(0, cli.run("user", "create", "secure", "-p", "s3cret",
            "-g", "admin", "-s", serverRoot));

      Properties users = loadProperties("users.properties");
      String stored = users.getProperty("secure");
      assertTrue(stored.contains(":"), "Encrypted password should contain algorithm:hash pairs");
   }

   @Test
   public void testCreateDuplicate() throws Exception {
      assertEquals(0, cli.run("user", "create", "dup", "-p", "pass1",
            "--plain-text", "-s", serverRoot));

      cli.run("user", "create", "dup", "-p", "pass2",
            "--plain-text", "-s", serverRoot);
      assertTrue(cli.shell().getBuffer().contains("already exists"));
   }

   @Test
   public void testRemoveUser() throws Exception {
      assertEquals(0, cli.run("user", "create", "toremove", "-p", "pass",
            "--plain-text", "-g", "ops", "-s", serverRoot));

      assertEquals(0, cli.run("user", "remove", "toremove", "-s", serverRoot));

      assertEquals(0, cli.run("user", "ls", "-s", serverRoot));
      assertTrue(cli.shell().getBuffer().contains("[]"));
   }

   @Test
   public void testChangePassword() throws Exception {
      assertEquals(0, cli.run("user", "create", "pwuser", "-p", "oldpass",
            "--plain-text", "-s", serverRoot));

      assertEquals(0, cli.run("user", "password", "pwuser", "-p", "newpass",
            "--plain-text", "-s", serverRoot));

      Properties users = loadProperties("users.properties");
      assertEquals("newpass", users.getProperty("pwuser"));
   }

   @Test
   public void testModifyGroups() throws Exception {
      assertEquals(0, cli.run("user", "create", "groupuser", "-p", "pass",
            "--plain-text", "-g", "readers", "-s", serverRoot));

      assertEquals(0, cli.run("user", "groups", "groupuser",
            "-g", "readers,writers,admins", "-s", serverRoot));

      assertEquals(0, cli.run("user", "describe", "groupuser", "-s", serverRoot));
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("readers"));
      assertTrue(output.contains("writers"));
      assertTrue(output.contains("admins"));
   }

   @Test
   public void testListUsers() throws Exception {
      for (String name : new String[]{"charlie", "alice", "bob"}) {
         assertEquals(0, cli.run("user", "create", name, "-p", "pass",
               "--plain-text", "-s", serverRoot));
      }

      assertEquals(0, cli.run("user", "ls", "-s", serverRoot));
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("alice"));
      assertTrue(output.contains("bob"));
      assertTrue(output.contains("charlie"));
   }

   @Test
   public void testListGroups() throws Exception {
      assertEquals(0, cli.run("user", "create", "u1", "-p", "pass",
            "--plain-text", "-g", "dev,ops", "-s", serverRoot));

      assertEquals(0, cli.run("user", "create", "u2", "-p", "pass",
            "--plain-text", "-g", "ops,qa", "-s", serverRoot));

      assertEquals(0, cli.run("user", "ls", "-g", "-s", serverRoot));
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("dev"));
      assertTrue(output.contains("ops"));
      assertTrue(output.contains("qa"));
   }

   @Test
   public void testEncryptAll() throws Exception {
      assertEquals(0, cli.run("user", "create", "user1", "-p", "pass1",
            "--plain-text", "-s", serverRoot));
      assertEquals(0, cli.run("user", "create", "user2", "-p", "pass2",
            "--plain-text", "-s", serverRoot));

      Properties before = loadProperties("users.properties");
      assertEquals("pass1", before.getProperty("user1"));
      assertEquals("pass2", before.getProperty("user2"));

      assertEquals(0, cli.run("user", "encrypt-all", "-s", serverRoot));

      Properties after = loadProperties("users.properties");
      assertTrue(after.getProperty("user1").contains(":"));
      assertTrue(after.getProperty("user2").contains(":"));
   }

   private Properties loadProperties(String filename) throws IOException {
      Properties props = new Properties();
      try (FileReader r = new FileReader(confDir.resolve(filename).toFile())) {
         props.load(r);
      }
      return props;
   }
}
