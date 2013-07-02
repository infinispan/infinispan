package org.infinispan.cli;

import java.util.regex.Matcher;

import org.infinispan.cli.shell.Man2Ansi;
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
}
