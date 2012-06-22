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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.infinispan.util.Util;
import org.testng.annotations.Test;

@Test(groups="functional", testName="cli.BatchingTest")
public class BatchingTest {

   public void testStandardInput() throws Exception {
      ByteArrayInputStream bais = null;
      ByteArrayOutputStream baos = null;
      InputStream in = System.in;
      PrintStream out = System.out;
      try {
         bais = new ByteArrayInputStream("version;\n".getBytes("UTF-8"));
         baos = new ByteArrayOutputStream();
         System.setIn(bais);
         System.setOut(new PrintStream(baos));
         Main.main(new String[]{"-f","-"});
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
