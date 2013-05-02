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
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License f  or more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter;

import java.util.Map;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName="cli-server.SessionTest")
public class SessionTest extends SingleCacheManagerTest {

   public void testSessionExpiration() throws Exception {
      Interpreter interpreter = new Interpreter();
      interpreter.initialize(this.cacheManager, TIME_SERVICE);
      interpreter.setSessionTimeout(500);
      interpreter.setSessionReaperWakeupInterval(1000);
      interpreter.start();

      try {
         String sessionId = interpreter.createSessionId(null);
         Thread.sleep(1500);
         Map<String, String> response = interpreter.execute(sessionId, "");
         assert response.containsKey("ERROR");
      } finally {
         interpreter.stop();
      }

   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createLocalCacheManager(false);
   }
}
