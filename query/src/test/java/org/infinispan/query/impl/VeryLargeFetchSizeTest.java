/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.query.impl;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Test whether LazyIterator and EagerIterator can handle a very large fetch size (Integer.MAX_VALUE). This makes sure
 * that neither of the two iterators actually try to create an array of size Integer.MAX_VALUE in such cases.
 *
 * @author Marko Luksa
 */
@Test(groups = "functional", testName = "query.impl.VeryLargeFetchSizeTest")
public class VeryLargeFetchSizeTest {

   public static final int VERY_LARGE_FETCH_SIZE = Integer.MAX_VALUE;

   private List<EntityInfo> entityInfos = new ArrayList<EntityInfo>();
   private AdvancedCache<String, String> cache;

   @Test
   public void testLazyIteratorHandlesVeryLargeFetchSize() throws IOException {
      cache = mock(AdvancedCache.class);
      DocumentExtractor extractor = mock(DocumentExtractor.class);
      new LazyIterator(extractor, new EntityLoader(cache, new KeyTransformationHandler()), VERY_LARGE_FETCH_SIZE);
   }

   @Test
   public void testEagerIteratorHandlesVeryLargeFetchSize() throws IOException {
      cache = mock(AdvancedCache.class);
      new EagerIterator(entityInfos, new EntityLoader(cache, new KeyTransformationHandler()), VERY_LARGE_FETCH_SIZE);
   }

}