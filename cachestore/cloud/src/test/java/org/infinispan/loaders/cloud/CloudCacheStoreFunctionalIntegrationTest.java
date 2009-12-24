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
package org.infinispan.loaders.cloud;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "unit", sequential = true, testName = "loaders.cloud.CloudCacheStoreFunctionalIntegrationTest")
public class CloudCacheStoreFunctionalIntegrationTest extends BaseCacheStoreFunctionalTest {

   private String proxyHost;
   private int proxyPort = -1;
   private int maxConnections = 20;
   private boolean isSecure = false;
   private String csBucket;
   private String accessKey;
   private String secretKey;
   private String cs;

   private static final String sysUsername = System.getProperty("infinispan.jclouds.username");
   private static final String sysPassword = System.getProperty("infinispan.jclouds.password");
   private static final String sysService = System.getProperty("infinispan.jclouds.service");

   @BeforeTest
   @Parameters({"infinispan.jclouds.username", "infinispan.jclouds.password", "infinispan.jclouds.service"})
   protected void setUpClient(@Optional String JcloudsUsername,
                              @Optional String JcloudsPassword,
                              @Optional String JcloudsService) throws Exception {

      accessKey = (JcloudsUsername == null) ? sysUsername : JcloudsUsername;
      secretKey = (JcloudsPassword == null) ? sysPassword : JcloudsPassword;
      cs = (JcloudsService == null) ? sysService : JcloudsService;

      if (accessKey == null || accessKey.trim().equals("") || secretKey == null || secretKey.trim().equals("")) {
         accessKey = "dummy";
         secretKey = "dummy";
      }
      csBucket = (System.getProperty("user.name") + "." + this.getClass().getSimpleName()).toLowerCase();
      System.out.printf("accessKey: %1$s, bucket: %2$s%n", accessKey, csBucket);
   }


   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      CloudCacheStoreConfig cfg = new CloudCacheStoreConfig();
      cfg.setCloudService(cs);
      cfg.setBucketPrefix(csBucket);
      cfg.setIdentity(accessKey);
      cfg.setPassword(secretKey);
      cfg.setProxyHost(proxyHost);
      cfg.setProxyPort(proxyPort);
      cfg.setSecure(isSecure);
      cfg.setMaxConnections(maxConnections);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      return cfg;
   }
}
