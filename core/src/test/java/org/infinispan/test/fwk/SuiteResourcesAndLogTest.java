/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.test.fwk;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * This class makes sure that all files are being deleted after each test run. It also logs testsuite information.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "test.fwk.SuiteResourcesAndLogTest")
public class SuiteResourcesAndLogTest {

   private static final Log log = LogFactory.getLog(SuiteResourcesAndLogTest.class);

   @BeforeSuite
   @AfterSuite
   public void printEnvInformation() {
      log("~~~~~~~~~~~~~~~~~~~~~~~~~ ENVIRONMENT INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");      
      log("jgroups.bind_addr = " + System.getProperty("jgroups.bind_addr"));
      log("java.runtime.version = " + System.getProperty("java.runtime.version"));
      log("java.runtime.name =" + System.getProperty("java.runtime.name"));
      log("java.vm.version = " + System.getProperty("java.vm.version"));
      log("java.vm.vendor = " + System.getProperty("java.vm.vendor"));
      log("os.name = " + System.getProperty("os.name"));
      log("os.version = " + System.getProperty("os.version"));
      log("sun.arch.data.model = " + System.getProperty("sun.arch.data.model"));
      log("sun.cpu.endian = " + System.getProperty("sun.cpu.endian"));
      log("protocol.stack = " + System.getProperty("protocol.stack"));
      log("infinispan.test.jgroups.protocol = " + System.getProperty("infinispan.test.jgroups.protocol"));
      log("infinispan.unsafe.allow_jdk8_chm = " + System.getProperty("infinispan.unsafe.allow_jdk8_chm"));
      String preferIpV4 = System.getProperty("java.net.preferIPv4Stack");
      log("java.net.preferIPv4Stack = " + preferIpV4);
      log("java.net.preferIPv6Stack = " + System.getProperty("java.net.preferIPv6Stack"));
      log("log4.configuration = " + System.getProperty("log4j.configuration"));
      log("MAVEN_OPTS = " + System.getProperty("MAVEN_OPTS"));
      log("~~~~~~~~~~~~~~~~~~~~~~~~~ ENVIRONMENT INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
      
      DebuggingUnitTestNGListener.describeErrorsIfAny();
   }

   private void log(String s) {
      System.out.println(s);
      log.info(s);
   }
}
