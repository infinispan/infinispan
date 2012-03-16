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

package org.infinispan.lucene;

import java.util.List;

import org.infinispan.config.AdvancedExternalizerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verifies the externalizers of the lucene-directory module are registered automatically
 * 
 * @author Sanne Grinovero
 * @since 5.0
 */
public class ExternalizersEnabledTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration configuration = new Configuration();
      configuration.setCacheMode(Configuration.CacheMode.LOCAL);
      configuration.setInvocationBatchingEnabled(true);
      return TestCacheManagerFactory.createCacheManager(configuration);
   }
   
   @Test
   public void testExternalizerDefined() {
      List<AdvancedExternalizerConfig> advancedExternalizerConfigs = cacheManager.getGlobalConfiguration().getExternalizers();
      Assert.assertEquals(5, advancedExternalizerConfigs.size());
   }

}
