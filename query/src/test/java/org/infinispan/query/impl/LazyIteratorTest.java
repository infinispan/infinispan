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

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.search.query.engine.impl.EntityInfoImpl;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.test.Person;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;



/**
 * @author Navin Surtani
 */
@Test(groups = "functional")
public class LazyIteratorTest {
   AdvancedCache<String, Person> cache;
   LazyIterator iterator = null;
   int fetchSize = 1;
   StringBuilder builder;
   Map<String, Person> dummyDataMap;
   Person[] persons; //position zero unused!
   List<String> keyList;
   private DocumentExtractor extractor;

   @BeforeTest
   public void setUpBeforeTest() throws Exception {
      dummyDataMap = new HashMap<String, Person>();
      keyList = new ArrayList<String>();
      persons = new Person[11];
      // Create a bunch of Person instances.
      // Set the fields to something so that everything will be found.
      for (int i = 1; i < 11; i++) {
         Person person = new Person();
         person.setBlurb("cat");
         // Stick them all into a dummy map.
         dummyDataMap.put("key" + i, person);
         keyList.add("S:key" + i);
         persons[i] = person;
      }
   }

   @BeforeMethod
   public void setUp() throws IOException {

      // Setting up the cache mock instance
      cache = mock(AdvancedCache.class);

      when(cache.get(anyObject())).thenAnswer(new Answer<Person>() {

         @Override
         public Person answer(InvocationOnMock invocation) throws Throwable {
            String key = invocation.getArguments()[0].toString();
            return dummyDataMap.get(key);
         }
      });

      when(cache.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
      when(cache.getAdvancedCache()).thenReturn(cache);

      extractor = mock(DocumentExtractor.class);
      HSQuery hsQuery = mock(HSQuery.class);
      when(hsQuery.queryDocumentExtractor()).thenReturn(extractor);
      when(hsQuery.queryResultSize()).thenReturn(dummyDataMap.size());
      when(extractor.extract(anyInt())).thenAnswer(new Answer<EntityInfo>() {
         @Override
         public EntityInfo answer(InvocationOnMock invocation) throws Throwable {
            int index = (Integer) invocation.getArguments()[0];
            String keyString = keyList.get(index);
            return new EntityInfoImpl(Person.class, keyString, keyString, new String[0]);
         }
      });

      iterator = new LazyIterator(hsQuery, new EntityLoader(cache, new KeyTransformationHandler()), fetchSize);
   }

   @AfterMethod(alwaysRun = false)
   public void tearDown() {
      verify(extractor).close();
      iterator = null;
   }

   public void testJumpToResult() throws IndexOutOfBoundsException {
      iterator.jumpToResult(0);
      assert iterator.isFirst();

      iterator.jumpToResult(1);
      assert iterator.isAfterFirst();

      iterator.jumpToResult(9);
      assert iterator.isLast();

      iterator.jumpToResult(8);
      assert iterator.isBeforeLast();
      iterator.close();
   }

   @Test(expectedExceptions = IndexOutOfBoundsException.class)
   public void testOutOfBoundsBelow() {
      try {
         iterator.jumpToResult(-1);
      }
      finally {
         iterator.close();
      }
   }

   @Test(expectedExceptions = IndexOutOfBoundsException.class)
   public void testOutOfBoundsAbove() {
      try {
         iterator.jumpToResult(keyList.size() + 1);
      }
      finally {
         iterator.close();
      }
   }

   public void testFirst() {
      assert iterator.isFirst() : "We should be pointing at the first element";
      Object next = iterator.next();

      assert next == persons[1];
      assert !iterator.isFirst();

      iterator.first();

      assert iterator.isFirst() : "We should be pointing at the first element";
      next = iterator.next();
      assert next == persons[1];
      assert !iterator.isFirst();
      iterator.close();
   }

   public void testLast() {
      //Jumps to the last element
      iterator.last();

      //Makes sure that the iterator is pointing at the last element.
      assert iterator.isLast();

      iterator.first();

      //Check that the iterator is NOT pointing at the last element.
      assert !iterator.isLast();

      iterator.close();
   }

   public void testAfterFirst() {
      //Jump to the second element.
      iterator.afterFirst();

      //Check this
      assert iterator.isAfterFirst();

      //Previous element in the list
      Object previous = iterator.previous();

      //Check that previous is the first element.
      assert previous == persons[2];

      //Make sure that the iterator isn't pointing at the second element.
      assert !iterator.isAfterFirst();

      iterator.close();
   }

   public void testBeforeLast() {
      //Jump to the penultimate element.
      iterator.beforeLast();

      //Check this
      assert iterator.isBeforeLast();

      //Next element - which should be the last.
      Object next = iterator.next();

      //Check that next is the penultimate element.
      assert next == persons[9];

      //Make sure that the iterator is not pointing at the penultimate element.
      assert !iterator.isBeforeLast();

      iterator.close();
   }

   public void testIsFirst() {
      iterator.first();
      assert iterator.isFirst();

      iterator.next();
      assert !iterator.isFirst();

      iterator.close();
   }

   public void testIsLast() {
      iterator.last();
      assert iterator.isLast();

      iterator.previous();
      assert !iterator.isLast();

      iterator.close();
   }

   public void testIsAfterFirst() {
      iterator.afterFirst();
      assert iterator.isAfterFirst();

      iterator.previous();
      assert !iterator.isAfterFirst();

      iterator.close();
   }

   public void testIsBeforeLast() {
      iterator.beforeLast();
      assert iterator.isBeforeLast();

      iterator.close();
   }

   public void testNextAndHasNext() {
      iterator.first();

      // This is so that we can "rebuild" the keystring for the nextAndHasNext and the previousAndHasPrevious methods.
      builder = new StringBuilder();

      for (int i = 1; i <= 10; i++) {
         builder.delete(0, 4);      // In this case we know that there are 4 characters in this string. so each time we come into the loop we want to clear the builder.
         builder.append("key");
         builder.append(i);
         String keyString = builder.toString();
         Object expectedValue = cache.get(keyString);
         assert iterator.hasNext(); // should have next as long as we are less than the number of elements.

         Object next = iterator.next();

         assert expectedValue == next; // tests next()
      }
      assert !iterator.hasNext(); // this should now NOT be true.

      iterator.close();
   }

   public void testPreviousAndHasPrevious() {
      iterator.last();

      // This is so that we can "rebuild" the keystring for the nextAndHasNext and the previousAndHasPrevious methods.
      builder = new StringBuilder();

      for (int i = 10; i >= 1; i--) {
         builder.delete(0, 5); // In this case we know that there are 4 characters in this string. so each time we come into the loop we want to clear the builder.
         builder.append("key");
         builder.append(i);
         String keyString = builder.toString();


         Object expectedValue = cache.get(keyString);

         assert iterator.hasPrevious(); // should have previous as long as we are less than the number of elements.

         Object previous = iterator.previous();

         assert expectedValue == previous; // tests previous()
      }
      assert !iterator.hasPrevious(); // this should now NOT be true.

      iterator.close();
   }

   public void testNextIndex() {
      iterator.first();
      assert iterator.nextIndex() == 1;

      iterator.last();
      assert iterator.nextIndex() == 10; //Index will be the index of the last element + 1.

      iterator.close();
   }

   public void testPreviousIndex() {
      iterator.first();
      assert iterator.previousIndex() == -1;

      iterator.last();
      assert iterator.previousIndex() == 8; //Index will be that of the last element - 1.

      iterator.close();
   }

}
