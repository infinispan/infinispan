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
package org.infinispan.jmx;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Test(groups = "functional", testName = "jmx.CustomMBeanServerPropertiesTest")
public class CustomMBeanServerPropertiesTest extends AbstractInfinispanTest {
   public void testDeclarativeCustomMBeanServerLookupProperties() throws IOException {
      String cfg = "<infinispan>" +
              "<global>" +
              "<globalJmxStatistics enabled=\"true\" mBeanServerLookup=\"" + TestLookup.class.getName() + "\">" +
              "<properties>" +
              "<property name=\"key\" value=\"value\"/>" +
              "</properties>" +
              "</globalJmxStatistics>" +
              "</global>" +
              "<default><jmxStatistics enabled=\"true\"/></default>" +
              "</infinispan>";
      InputStream stream = new ByteArrayInputStream(cfg.getBytes());
      CacheContainer cc = null;
      try {
         cc = TestCacheManagerFactory.fromStream(stream);
         cc.getCache();
         assert "value".equals(TestLookup.p.get("key"));
      } finally {
         TestingUtil.killCacheManagers(cc);
      }
   }

   public void testProgrammaticCustomMBeanServerLookupProperties() {
      CacheContainer cc = null;
      try {
         GlobalConfiguration gc = new GlobalConfiguration();
         TestLookup mbsl = new TestLookup();
         gc.setMBeanServerLookupInstance(mbsl);
         Properties p = new Properties();
         p.setProperty("key", "value");
         gc.setMBeanServerProperties(p);
         Configuration cfg = new Configuration();
         cfg.setExposeJmxStatistics(true);
         cc = TestCacheManagerFactory.createCacheManager(gc, cfg);
         cc.getCache();
         assert "value".equals(mbsl.localProps.get("key"));
      } finally {
         TestingUtil.killCacheManagers(cc);
      }
   }

   public static class TestLookup implements MBeanServerLookup {

      static Properties p;
      Properties localProps;

      @Override
      public MBeanServer getMBeanServer(Properties properties) {
         TestLookup.p = properties;
         localProps = properties;
         return new PerThreadMBeanServerLookup().getMBeanServer(p);
      }
   }
}


