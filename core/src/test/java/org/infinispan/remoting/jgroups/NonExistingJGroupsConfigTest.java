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
package org.infinispan.remoting.jgroups;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "remoting.jgroups.NonExistingJGroupsConfigTest", groups = "functional")
public class NonExistingJGroupsConfigTest extends AbstractInfinispanTest {
   
   public void channelLookupTest() throws Exception {
      String config = INFINISPAN_START_TAG +
      "   <global>\n" +
      "      <transport clusterName=\"demoCluster\">\n" +
      " <properties> \n" +
      "<property name=\"configurationFile\" value=\"nosuchfile.xml\"/> \n" +
      "</properties> \n" +
      "</transport> \n " +
      "   </global>\n" +
      "\n" +
      "   <default>\n" +
      "      <clustering mode=\"replication\">\n" +
      "      </clustering>\n" +
      "   </default>\n" +
      TestingUtil.INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(new DefaultCacheManager(is)) {
         @Override
         public void call() throws Exception {
            try {
               cm.getCache();
            } catch (Exception e) {
               assert e.getCause().getCause().getCause() instanceof FileNotFoundException;
            }
         }
      });
   }
}
