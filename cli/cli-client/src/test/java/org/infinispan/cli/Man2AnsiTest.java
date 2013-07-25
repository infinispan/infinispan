package org.infinispan.cli;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;

import org.infinispan.cli.shell.Man2Ansi;
import org.infinispan.commons.util.Util;
import org.testng.annotations.Test;

@Test(groups="functional", testName="cli.shell.Man2AnsiTest")
public class Man2AnsiTest {
   public void testMacro() {
      Matcher matcher = Man2Ansi.MAN_MACRO_REGEX.matcher(".SH SYNOPSIS");
      assert matcher.matches();
      assert matcher.groupCount()==2;
      assert ".SH ".equals(matcher.group(1));
      assert "SYNOPSIS".equals(matcher.group(2));
   }

   public void testNoMacro() {
      Matcher matcher = Man2Ansi.MAN_MACRO_REGEX.matcher("Text");
      assert matcher.matches();
      assert matcher.groupCount()==2;
      assert matcher.group(1)==null;
      assert "Text".equals(matcher.group(2));
   }

   public void testAllManPages() throws Exception {
      InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("help");
      BufferedReader r = new BufferedReader(new InputStreamReader(is));
      for(String name = r.readLine(); name != null; name = r.readLine()) {
         testManPage("help/"+name);
      }
      r.close();
   }

   private void testManPage(String name) throws Exception {
      InputStream is = null;
      try {
         is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
         Man2Ansi man2ansi = new Man2Ansi(72);
         man2ansi.render(is);
      } finally {
         Util.close(is);
      }
   }
}
