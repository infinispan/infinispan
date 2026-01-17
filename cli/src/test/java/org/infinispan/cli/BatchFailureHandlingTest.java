package org.infinispan.cli;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Properties;

import org.infinispan.cli.commands.CLI;
import org.infinispan.commons.util.Util;
import org.infinispan.testing.Testing;
import org.junit.Test;

public class BatchFailureHandlingTest {
   @Test
   public void testBatchFailureHandling() {
      File workingDir = new File(Testing.tmpDirectory(BatchFailureHandlingTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      AeshTestShell shell = new AeshTestShell();
      String path = BatchFailureHandlingTest.class.getClassLoader().getResource("fail.batch").getPath();
      assertEquals(1, CLI.main(shell, properties, "--on-error=FAIL_FAST", "-f", path));
      shell.assertContains("fail.batch, line 1: 'fail'");
      shell.clear();
      assertEquals(0, CLI.main(shell, properties, "--on-error=IGNORE", "-f", path));
      shell.assertContains("fail.batch, line 1: 'fail'");
      shell.assertContains("pass");
      shell.clear();
      assertEquals(1, CLI.main(shell, properties, "--on-error=FAIL_AT_END", "-f", path));
      shell.assertContains("fail.batch, line 1: 'fail'");
      shell.assertContains("pass");
      shell.clear();
   }
}
