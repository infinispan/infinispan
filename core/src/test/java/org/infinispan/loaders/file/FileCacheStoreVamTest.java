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
package org.infinispan.loaders.file;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;

/**
 * FileCacheStoreTest using production level marshaller.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "unit", testName = "loaders.file.FileCacheStoreVamTest")
public class FileCacheStoreVamTest extends FileCacheStoreTest {
   private EmbeddedCacheManager cm;

   @Override
   protected StreamingMarshaller getMarshaller() {
      if (cm == null)
         cm = TestCacheManagerFactory.createLocalCacheManager(false);

      return extractCacheMarshaller(cm.getCache());
   }

   @AfterTest
   public void destroy() {
      if (cm != null) cm.stop();
   }

}
