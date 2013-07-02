package org.infinispan.cli;

import org.infinispan.cli.commands.ProcessedCommand;
import org.testng.annotations.Test;

@Test(groups="functional", testName="cli.shell.ProcessedCommandTest")
public class ProcessedCommandTest {

   public void testArgumentParsing() {
      ProcessedCommand pc = new ProcessedCommand("cmd abc");
      assert "cmd".equals(pc.getCommand());

      assert pc.getArguments().size()==1;

      assert "abc".equals(pc.getArguments().get(0).getValue());
   }

   public void testQuotedArgumentParsing() {
      ProcessedCommand pc = new ProcessedCommand("cmd \"abc\" \"def\"");
      assert "cmd".equals(pc.getCommand());

      assert pc.getArguments().size()==2;

      assert "abc".equals(pc.getArguments().get(0).getValue());
      assert "def".equals(pc.getArguments().get(1).getValue());
   }

   public void testMixedArgumentParsing() {
      ProcessedCommand pc = new ProcessedCommand("cmd \"abc\" 'def' ghi");
      assert "cmd".equals(pc.getCommand());

      assert pc.getArguments().size()==3;

      assert "abc".equals(pc.getArguments().get(0).getValue());
      assert "def".equals(pc.getArguments().get(1).getValue());
      assert "ghi".equals(pc.getArguments().get(2).getValue());
   }

   public void testNoArguments() {
      ProcessedCommand pc = new ProcessedCommand("cmd ");
      assert "cmd".equals(pc.getCommand());

      assert pc.getArguments().size()==0;

      assert pc.isCommandComplete();
   }
}
