/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
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
