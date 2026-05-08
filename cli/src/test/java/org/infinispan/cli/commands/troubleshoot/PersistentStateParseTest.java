package org.infinispan.cli.commands.troubleshoot;

import static org.infinispan.testing.Testing.tmpDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import org.infinispan.cli.AeshTestShell;
import org.infinispan.cli.commands.CLI;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.FileSystemLock;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.testing.Testing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class PersistentStateParseTest {

   private static final String SAMPLE_STATE_CONTENT = "key=content";

   @Test
   public void testListAllStates(TestInfo testInfo) throws IOException {
      File workingDir = new File(Testing.tmpDirectory(PersistentStateParseTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(testInfo.getTestMethod().get().getName(), persistentStateName);

      AeshTestShell shell = new AeshTestShell();
      int exit = CLI.main(shell, properties, "troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString());
      assertEquals(0, exit);
      shell.assertContains(persistentStateName);
   }

   @Test
   public void showStateContents(TestInfo testInfo) throws IOException {
      File workingDir = new File(Testing.tmpDirectory(PersistentStateParseTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(testInfo.getTestMethod().get().getName(), persistentStateName);

      AeshTestShell shell = new AeshTestShell();
      int exit = CLI.main(shell, properties, "troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString(), "--show", persistentStateName);
      assertEquals(0, exit);
      shell.assertContains("key=key; value=content");
   }

   @Test
   public void testSuccessfulDeleteScope(TestInfo testInfo) throws IOException {
      File workingDir = new File(Testing.tmpDirectory(PersistentStateParseTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(testInfo.getTestMethod().get().getName(), persistentStateName);

      // Assert the file exists before submitting command.
      assertTrue(p.toFile().exists());

      AeshTestShell shell = new AeshTestShell();
      int exit = CLI.main(shell, properties, "troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString(), "--delete", persistentStateName);
      assertEquals(0, exit);
      shell.assertContains("key=key; value=content");

      // Assert the file was deleted.
      assertFalse(p.toFile().exists());
   }

   @Test
   public void testFailedDeleteOnGlobalLock(TestInfo testInfo) throws IOException {
      File workingDir = new File(Testing.tmpDirectory(PersistentStateParseTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(testInfo.getTestMethod().get().getName(), persistentStateName);

      // Assert the file exists before submitting command.
      assertTrue(p.toFile().exists());

      FileSystemLock lock = new FileSystemLock(p.toAbsolutePath().getParent(), ScopedPersistentState.GLOBAL_SCOPE);
      assertTrue(lock.tryLock());
      try {
         AeshTestShell shell = new AeshTestShell();
         int exit = CLI.main(shell, properties, "troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString(), "--delete", persistentStateName);
         assertEquals(0, exit);
      } finally {
         lock.unlock();
      }

      // Assert the file is still present.
      assertTrue(p.toFile().exists());
   }

   private Path createPersistentState(String parent, String name) throws IOException {
      String p = tmpDirectory(PersistentStateParseTest.class.getName(), parent);
      Util.recursiveFileRemove(p);
      Path state = Path.of(p, name + ".state");
      state.toFile().getParentFile().mkdirs();
      state.toFile().createNewFile();
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(state.toFile()))) {
         writer.write(SAMPLE_STATE_CONTENT);
      }
      return state;
   }
}
