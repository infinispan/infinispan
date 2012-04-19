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
