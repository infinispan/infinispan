package org.infinispan.cli.commands.troubleshoot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.infinispan.cli.test.CliExtension;
import org.infinispan.cli.test.CliTerminal;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.FileSystemLock;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

@Cli
public class PersistentStateParseTest {

   private static final String SAMPLE_STATE_CONTENT = "key=content";

   @RegisterExtension
   CliExtension cli = new CliExtension();

   @Test
   public void testListAllStates(TestInfo testInfo) throws IOException {
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(testInfo.getTestMethod().get().getName(), persistentStateName);
      try (CliTerminal terminal = cli.run("troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString())) {
         assertEquals(0, terminal.exitCode());
         terminal.assertContains(persistentStateName);
      }
   }

   @Test
   public void showStateContents(TestInfo testInfo) throws IOException {
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(testInfo.getTestMethod().get().getName(), persistentStateName);
      try (CliTerminal terminal = cli.run("troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString(), "--show", persistentStateName)) {
         assertEquals(0, terminal.exitCode());
         terminal.assertContains("key=key; value=content");
      }
   }

   @Test
   public void testSuccessfulDeleteScope(TestInfo testInfo) throws IOException {
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(testInfo.getTestMethod().get().getName(), persistentStateName);
      assertThat(p).exists();
      try (CliTerminal terminal = cli.run("troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString(), "--delete", persistentStateName)) {
         assertEquals(0, terminal.exitCode());
         terminal.assertContains("key=key; value=content");
      }
      assertThat(p).doesNotExist();
   }

   @Test
   public void testFailedDeleteOnGlobalLock(TestInfo testInfo) throws IOException {
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(testInfo.getTestMethod().get().getName(), persistentStateName);
      assertThat(p).exists();
      FileSystemLock lock = new FileSystemLock(p.toAbsolutePath().getParent(), ScopedPersistentState.GLOBAL_SCOPE);
      assertTrue(lock.tryLock());
      try (CliTerminal terminal = cli.run("troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString(), "--delete", persistentStateName)) {
         assertEquals(1, terminal.exitCode());
      } finally {
         lock.unlock();
      }
      assertThat(p).exists();
   }

   private Path createPersistentState(String parent, String name) throws IOException {
      String p = tmpDirectory(PersistentStateParseTest.class.getName(), parent);
      Util.recursiveFileRemove(p);
      Path state = Path.of(p, name + ".state");
      Files.createDirectories(state.getParent());
      Files.createFile(state);
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(state.toFile()))) {
         writer.write(SAMPLE_STATE_CONTENT);
      }
      return state;
   }
}
