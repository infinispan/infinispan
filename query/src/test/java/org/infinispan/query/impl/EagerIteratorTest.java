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
import org.infinispan.query.QueryIterator;
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

@Test(groups = "functional")
public class EagerIteratorTest {
   List<String> keys;
   List<EntityInfo> entityInfos;
   Map<String, String> dummyResults;
   QueryIterator iterator;
   int fetchSize = 1;
   AdvancedCache<String, String> cache;
   private KeyTransformationHandler keyTransformationHandler;

   @BeforeMethod
   public void setUp() throws Exception {

      // create a set of dummy keys
      keys = new ArrayList<String>();
      // create some dummy data
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

      iterator = new EagerIterator(entityInfos, new EntityLoader(cache, keyTransformationHandler), fetchSize);
   }

   @AfterMethod (alwaysRun = true)
   public void tearDown() {
      keys = null;
      dummyResults = null;
      iterator = null;
   }

   public void testJumpToResult() throws IndexOutOfBoundsException {
      iterator.jumpToResult(0);
      assert iterator.isFirst();

      iterator.jumpToResult(1);
      assert iterator.isAfterFirst();

      iterator.jumpToResult((keys.size() - 1));
      assert iterator.isLast();

      iterator.jumpToResult(keys.size() - 2);
      assert iterator.isBeforeLast();
   }

   public void testFirst() {
      assert iterator.isFirst() : "We should be pointing at the first element";
      Object next = iterator.next();

      assert next == dummyResults.get(keys.get(0));

      assert !iterator.isFirst();

      iterator.first();

      assert iterator.isFirst() : "We should be pointing at the first element";
      next = iterator.next();
      assert next == dummyResults.get(keys.get(0));
      assert !iterator.isFirst();

   }

   public void testLast() {
      //Jumps to the last element
      iterator.last();

      //Makes sure that the iterator is pointing at the last element.
      assert iterator.isLast();

      Object next = iterator.next();

      //Returns the size of the list of keys.
      int size = keys.size();

      //Makes sure that previous is the last element.
      assert next == dummyResults.get(keys.get(size - 1));

      //Check that the iterator is NOT pointing at the last element.
      assert !iterator.isLast();
   }

   public void testAfterFirst() {
      //Jump to the second element.
      iterator.afterFirst();

      //Check this
      assert iterator.isAfterFirst();

      //Previous element in the list
      Object previous = iterator.previous();

      //Check that previous is the first element.
      assert previous == dummyResults.get(keys.get(1));

      //Make sure that the iterator isn't pointing at the second element.
      assert !iterator.isAfterFirst();

   }

   public void testBeforeLast() {
      //Jump to the penultimate element.
      iterator.beforeLast();

      //Check this
      assert iterator.isBeforeLast();

      //Next element - which should be the last.
      Object next = iterator.next();

      //Check that next is the penultimate element.
      int size = keys.size();
      assert next == dummyResults.get(keys.get(size - 2));

      //Make sure that the iterator is not pointing at the penultimate element.
      assert !iterator.isBeforeLast();
   }

   public void testIsFirst() {
      iterator.first();
      assert iterator.isFirst();

      iterator.next();
      assert !iterator.isFirst();
   }

   public void testIsLast() {
      iterator.last();
      assert iterator.isLast();

      iterator.previous();
      assert !iterator.isLast();
   }

   public void testIsAfterFirst() {
      iterator.afterFirst();
      assert iterator.isAfterFirst();

      iterator.previous();
      assert !iterator.isAfterFirst();
   }

   public void testIsBeforeLast() {
      iterator.beforeLast();
      assert iterator.isBeforeLast();
   }

   public void testNextAndHasNext() {
      iterator.first();
      for (int i = 0; i < keys.size(); i++) {
         Object expectedValue = dummyResults.get(keys.get(i));
         assert iterator.hasNext(); // should have next as long as we are less than the number of elements.
         assert expectedValue == iterator.next(); // tests next()
      }
      assert !iterator.hasNext(); // this should now NOT be true.
   }

   public void testPreviousAndHasPrevious() {
      iterator.last();
      for (int i = keys.size() - 1; i >= 0; i--) {
         Object expectedValue = dummyResults.get(keys.get(i));
         assert iterator.hasPrevious(); // should have previous as long as we are more than the number of elements.
         assert expectedValue == iterator.previous(); // tests previous()
      }
      assert !iterator.hasPrevious(); // this should now NOT be true.

   }

   public void testNextIndex() {
      iterator.first();
      assert iterator.nextIndex() == 1;

      iterator.last();
      assert iterator.nextIndex() == keys.size();

   }

   public void testPreviousIndex() {
      iterator.first();
      assert iterator.previousIndex() == -1;

      iterator.last();
      assert iterator.previousIndex() == (keys.size() - 2);
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
