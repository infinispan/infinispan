/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.test.testng;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
   
   @Override
   public void onFinish(ITestContext testCxt) {
      CacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(new Configuration());
      try {
         cm.start();
         try {
            TestingUtil.blockUntilViewReceived(cm.getCache(), 1, 2000, true);
         } catch (RuntimeException re) {
            System.out.println("CacheManagers alive after test! - " + testCxt.getName() + " " + re.getMessage());
         }
      }
      finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

}
