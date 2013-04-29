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
package org.infinispan.cli.io;

import java.io.IOException;

import org.fusesource.jansi.Ansi;
import org.jboss.aesh.console.Console;
import org.jboss.aesh.console.Prompt;

public class ConsoleIOAdapter implements IOAdapter {
   private final Console console;

   public ConsoleIOAdapter(final Console console) {
      this.console = console;
   }

   @Override
   public boolean isInteractive() {
      return true;
   }

   @Override
   public String readln(String prompt) throws IOException {
      return console.read(prompt).getBuffer();
   }

   @Override
   public String secureReadln(String prompt) throws IOException {
      return console.read(new Prompt(prompt), (char) 0).getBuffer();
   }

   @Override
   public void println(String s) throws IOException {
      console.pushToStdOut(s);
      console.pushToStdOut("\n");
   }

   @Override
   public void error(String s) throws IOException {
      Ansi ansi = new Ansi();
      ansi.fg(Ansi.Color.RED);
      println(ansi.render(s).reset().toString());
   }

   @Override
   public int getWidth() {
      return console.getTerminalSize().getWidth();
   }

   @Override
   public void close() throws IOException {
      console.stop();
   }

}
