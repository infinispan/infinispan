package org.infinispan.cli.commands.troubleshoot;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import org.infinispan.cli.AeshTestShell;
import org.infinispan.cli.CliPipeTest;
import org.infinispan.cli.commands.CLI;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.FileSystemLock;
import org.infinispan.globalstate.ScopedPersistentState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class PersistentStateParseTest {

   private static final String SAMPLE_STATE_CONTENT = "key=content";

   @Rule
   public TestName name = new TestName();

   @Test
   public void testListAllStates() throws IOException {
      File workingDir = new File(CommonsTestingUtil.tmpDirectory(CliPipeTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(name.getMethodName(), persistentStateName);

      AeshTestShell shell = new AeshTestShell();
      int exit = CLI.main(shell, properties, "troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString());
      assertEquals(0, exit);
      shell.assertContains(persistentStateName);
   }

   @Test
   public void showStateContents() throws IOException {
      File workingDir = new File(CommonsTestingUtil.tmpDirectory(CliPipeTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(name.getMethodName(), persistentStateName);

      AeshTestShell shell = new AeshTestShell();
      int exit = CLI.main(shell, properties, "troubleshoot", "persistent-state", p.toAbsolutePath().getParent().toString(), "--show", persistentStateName);
      assertEquals(0, exit);
      shell.assertContains("key=key; value=content");
   }

   @Test
   public void testSuccessfulDeleteScope() throws IOException {
      File workingDir = new File(CommonsTestingUtil.tmpDirectory(CliPipeTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(name.getMethodName(), persistentStateName);

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
   public void testFailedDeleteOnGlobalLock() throws IOException {
      File workingDir = new File(CommonsTestingUtil.tmpDirectory(CliPipeTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      String persistentStateName = UUID.randomUUID().toString();
      Path p = createPersistentState(name.getMethodName(), persistentStateName);

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
