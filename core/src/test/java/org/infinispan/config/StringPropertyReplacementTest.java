/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.config;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * /**
 * Tests that string property replacement works properly when parsing
 * a config file.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "config.StringPropertyReplacementTest")
public class StringPropertyReplacementTest extends SingleCacheManagerTest {


   protected EmbeddedCacheManager createCacheManager() throws Exception {
      System.setProperty("test.property.asyncListenerMaxThreads","2");
      System.setProperty("test.property.IsolationLevel","READ_COMMITTED");
      System.setProperty("test.property.writeSkewCheck","true");
      System.setProperty("test.property.SyncCommitPhase","true");
      return TestCacheManagerFactory.fromXml("configs/string-property-replaced.xml");
   }

   public void testGlobalConfig() {
      Properties asyncListenerExecutorProperties = cacheManager.getGlobalConfiguration().getAsyncListenerExecutorProperties();
      asyncListenerExecutorProperties.get("maxThreads").equals("2");

      Properties transportProps = cacheManager.getGlobalConfiguration().getTransportProperties();
      assert transportProps.get("configurationFile").equals("jgroups-tcp.xml");
   }

   public void testDefaultCache() {
      Configuration configuration = cacheManager.getCache().getConfiguration();
      assert configuration.getIsolationLevel().equals(IsolationLevel.READ_COMMITTED);
      assert !configuration.isWriteSkewCheck();      
      assert configuration.isSyncCommitPhase();
   }
}

