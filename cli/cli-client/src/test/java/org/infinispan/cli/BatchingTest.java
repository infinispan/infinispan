package org.infinispan.cli;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.infinispan.cli.shell.Shell;
import org.infinispan.cli.shell.ShellImpl;
import org.infinispan.commons.util.Util;
import org.testng.annotations.Test;

@Test(groups="functional", testName="cli.BatchingTest")
public class BatchingTest {

   public void testStandardInput() throws Exception {
      ByteArrayInputStream bais = null;
      ByteArrayOutputStream baos = null;
      InputStream in = System.in;
      PrintStream out = System.out;
      try {
         bais = new ByteArrayInputStream("version;\n".getBytes(UTF_8));
         baos = new ByteArrayOutputStream();
         System.setIn(bais);
         System.setOut(new PrintStream(baos));
         Shell shell = new ShellImpl();
         shell.init(new String[]{"-f","-"});
         shell.run();
         System.out.flush();
         String output = baos.toString("UTF-8");
         assert output.contains("Version");
      } finally {
         System.setIn(in);
         System.setOut(out);
         Util.close(bais, baos);
      }
   }
}
