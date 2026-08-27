package org.infinispan.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.infinispan.cli.test.CliExtension;
import org.infinispan.cli.test.CliTerminal;
import org.infinispan.commons.util.Util;
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
      Util.recursiveFileRemove(serverRoot);
      Files.createDirectories(confDir);
   }

   @Test
   public void testCreateAndDescribe() throws Exception {
      try (CliTerminal terminal = cli.run("user", "create", "admin", "-p", "changeme", "--plain-text", "-g", "administrators,deployers", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());

         Properties users = loadProperties("users.properties");
         assertEquals("changeme", users.getProperty("admin"));
      }
      try (CliTerminal terminal = cli.run("user", "describe", "admin", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("admin", "administrators", "deployers");
      }
   }

   @Test
   public void testCreateEncrypted() throws Exception {
      try (CliTerminal terminal = cli.run("user", "create", "secure", "-p", "s3cret", "-g", "admin", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      Properties users = loadProperties("users.properties");
      String stored = users.getProperty("secure");
      assertThat(stored).contains(":").describedAs("Encrypted password should contain algorithm:hash pairs");
   }

   @Test
   public void testCreateDuplicate() {
      try (CliTerminal terminal = cli.run("user", "create", "dup", "-p", "pass1", "--plain-text", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode(), terminal.output());
      }
      try (CliTerminal terminal = cli.run("user", "create", "dup", "-p", "pass2", "--plain-text", "-s", serverRoot)) {
         assertEquals(1, terminal.exitCode());
         assertThat(terminal.output()).contains("already exists");
      }
   }

   @Test
   public void testRemoveUser() {
      try (CliTerminal terminal = cli.run("user", "create", "toremove", "-p", "pass", "--plain-text", "-g", "ops", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("user", "remove", "toremove", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("user", "ls", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("[]");
      }
   }

   @Test
   public void testChangePassword() throws Exception {
      try (CliTerminal terminal = cli.run("user", "create", "pwuser", "-p", "oldpass", "--plain-text", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("user", "password", "pwuser", "-p", "newpass", "--plain-text", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      Properties users = loadProperties("users.properties");
      assertEquals("newpass", users.getProperty("pwuser"));
   }

   @Test
   public void testModifyGroups() {
      try (CliTerminal terminal = cli.run("user", "create", "groupuser", "-p", "pass", "--plain-text", "-g", "readers", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("user", "groups", "groupuser", "-g", "readers,writers,admins", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("user", "describe", "groupuser", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("readers", "writers", "admins");
      }
   }

   @Test
   public void testListUsers() {
      for (String name : new String[]{"charlie", "alice", "bob"}) {
         try (CliTerminal terminal = cli.run("user", "create", name, "-p", "pass", "--plain-text", "-s", serverRoot)) {
            assertEquals(0, terminal.exitCode());
         }
      }

      try (CliTerminal terminal = cli.run("user", "ls", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("alice", "bob", "charlie");
      }
   }

   @Test
   public void testListGroups() {
      try (CliTerminal terminal = cli.run("user", "create", "u1", "-p", "pass", "--plain-text", "-g", "dev,ops", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("user", "create", "u2", "-p", "pass", "--plain-text", "-g", "ops,qa", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("user", "ls", "-g", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
         assertThat(terminal.output()).contains("dev", "ops", "qa");
      }
   }

   @Test
   public void testEncryptAll() throws Exception {
      try (CliTerminal terminal = cli.run("user", "create", "user1", "-p", "pass1", "--plain-text", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      try (CliTerminal terminal = cli.run("user", "create", "user2", "-p", "pass2", "--plain-text", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
         Properties before = loadProperties("users.properties");
         assertEquals("pass1", before.getProperty("user1"));
         assertEquals("pass2", before.getProperty("user2"));
      }
      try (CliTerminal terminal = cli.run("user", "encrypt-all", "-s", serverRoot)) {
         assertEquals(0, terminal.exitCode());
      }
      Properties after = loadProperties("users.properties");
      assertThat(after.getProperty("user1")).contains(":");
      assertThat(after.getProperty("user2")).contains(":");
   }

   private Properties loadProperties(String filename) throws IOException {
      Properties props = new Properties();
      try (FileReader r = new FileReader(confDir.resolve(filename).toFile())) {
         props.load(r);
      }
      return props;
   }
}
