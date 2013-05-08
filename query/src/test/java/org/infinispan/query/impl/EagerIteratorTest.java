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
package org.infinispan.query.impl;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.AdvancedCache;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Navin Surtani
 *         <p/>
 *         Test class for the {@link EagerIterator}
 */

@Test(groups = "functional", testName = "query.impl.EagerIteratorTest")
public class EagerIteratorTest {
   List<String> keys;
   List<EntityInfo> entityInfos;
   Map<String, String> dummyResults;
   ResultIterator iterator;
   AdvancedCache<String, String> cache;
   private KeyTransformationHandler keyTransformationHandler;

   @BeforeMethod
   public void setUp() throws Exception {

      keys = new ArrayList<String>();
      dummyResults = new HashMap<String, String>();

      entityInfos = new ArrayList<EntityInfo>();
      keyTransformationHandler = new KeyTransformationHandler();

      for (int i = 1; i <= 10; i++) {
         String key = "key" + i;
         keys.add(key);
         entityInfos.add(new MockEntityInfo(keyTransformationHandler.keyToString(key)));
         dummyResults.put(key, "Result number " + i);
      }

      // create the instance of the iterator.
      cache = mock(AdvancedCache.class);

      when(cache.get(anyObject())).thenAnswer(new Answer<String>() {
         @Override
         public String answer(InvocationOnMock invocation) throws Throwable {
            String k = invocation.getArguments()[0].toString();
            return dummyResults.get(k);
         }

      });

      iterator = new EagerIterator(entityInfos, new EntityLoader(cache, keyTransformationHandler), getFetchSize());
   }

   protected int getFetchSize() {
      return 1;
   }

   @AfterMethod
   public void tearDown() {
      iterator.close();

      keys = null;
      dummyResults = null;
      iterator = null;
   }

   protected String resultAt(int index) {
      return dummyResults.get(keys.get( index ));
   }

   public void testNextAndHasNext() {
      for (int i = 0; i < keys.size(); i++) {
         Object expectedValue = resultAt(i);
         assert iterator.hasNext(); // should have next as long as we are less than the number of elements.
         assert expectedValue == iterator.next(); // tests next()
      }
      assert !iterator.hasNext(); // this should now NOT be true.
   }

   private static class MockEntityInfo implements EntityInfo {

      private final String key;

      public MockEntityInfo(String key) {
         this.key = key;
      }

      @Override
      public Class<?> getClazz() {
         return null;
      }

      @Override
      public Serializable getId() {
         return key;
      }

      @Override
      public String getIdName() {
         return null;
      }

      @Override
      public Object[] getProjection() {
         return new Object[0];
      }

      @Override
      public List<Integer> getIndexesOfThis() {
         return null;
      }

      @Override
      public boolean isProjectThis() {
         return false;
      }

      @Override
      public void populateWithEntityInstance(Object entity) {
      }
   }
}
