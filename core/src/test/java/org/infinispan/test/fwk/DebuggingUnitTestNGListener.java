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

import java.util.Set;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.ITestContext;

/**
 * DebuggingUnitTestNGListener is a slower version of UnitTestTestNGListener
 * containing some additional sanity checks of the tests themselves.
 * It will verify any clustered CacheManager created by the test was properly killed,
 * if not a message is output.
 * 
 * NOTE: The test WILL NOT FAIL when not cleaning up, you'll have to check for these messages in logs.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public class DebuggingUnitTestNGListener extends UnitTestTestNGListener {
   
   private static final Log log = LogFactory.getLog(DebuggingUnitTestNGListener.class);
   
   private static final Set<String> failedTestDescriptions = new ConcurrentHashSet<String>();
   
   @Override
   public void onFinish(ITestContext testCxt) {
      super.onFinish(testCxt);
      checkCleanedUp(testCxt);
   }
   
   private void checkCleanedUp(ITestContext testCxt) {
      CacheContainer cm = TestCacheManagerFactory.createClusteredCacheManager(new Configuration());
      try {
         cm.start();
         try {
            TestingUtil.blockUntilViewReceived(cm.getCache(), 1, 2000, true);
         } catch (RuntimeException re) {
            failedTestDescriptions.add(
                     "CacheManagers alive after test! - " + testCxt.getName() + " " + re.getMessage()
                     );
         }
      }
      finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public static void describeErrorsIfAny() {
      if ( ! failedTestDescriptions.isEmpty() ) {
         log("~~~~~~~~~~~~~~~~~~~~~~~~~ TEST HEALTH INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
         log("Some tests didn't properly shutdown the CacheManager:");
         for (String errorMsg : failedTestDescriptions) {
            System.out.println( "\t" + errorMsg);
         }
         log("~~~~~~~~~~~~~~~~~~~~~~~~~ TEST HEALTH INFO ~~~~~~~~~~~~~~~~~~~~~~~~~~");
      }
   }
   
   private static void log(String s) {
      System.out.println(s);
      log.info(s);
   }

}
