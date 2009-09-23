package org.infinispan.query.impl;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.easymock.IAnswer;
import org.hibernate.search.engine.DocumentExtractor;
import org.hibernate.search.engine.EntityInfo;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.infinispan.Cache;
import org.infinispan.query.test.Person;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author Navin Surtani
 */
@Test(groups = "functional")
public class LazyIteratorTest {
   Cache<String, Person> cache;
   LazyIterator iterator = null;
   int fetchSize = 1;
   Person person1, person2, person3, person4, person5, person6, person7, person8, person9, person10;
   StringBuilder builder;
   Map<String, Person> dummyDataMap;
   List<String> keyList;

   @BeforeTest
   public void setUpBeforeTest() throws Exception {
      dummyDataMap = new HashMap<String, Person>();

      // Create a bunch of Person instances.
      person1 = new Person();
      person2 = new Person();
      person3 = new Person();
      person4 = new Person();
      person5 = new Person();
      person6 = new Person();
      person7 = new Person();
      person8 = new Person();
      person9 = new Person();
      person10 = new Person();


      // Set the fields to something so that everything will be found.
      person1.setBlurb("cat");
      person2.setBlurb("cat");
      person3.setBlurb("cat");
      person4.setBlurb("cat");
      person5.setBlurb("cat");
      person6.setBlurb("cat");
      person7.setBlurb("cat");
      person8.setBlurb("cat");
      person9.setBlurb("cat");
      person10.setBlurb("cat");

      // Stick them all into a dummy map.
      dummyDataMap.put("key1", person1);
      dummyDataMap.put("key2", person2);
      dummyDataMap.put("key3", person3);
      dummyDataMap.put("key4", person4);
      dummyDataMap.put("key5", person5);
      dummyDataMap.put("key6", person6);
      dummyDataMap.put("key7", person7);
      dummyDataMap.put("key8", person8);
      dummyDataMap.put("key9", person9);
      dummyDataMap.put("key10", person10);

      keyList = new ArrayList<String>();

      keyList.add("key1");
      keyList.add("key2");
      keyList.add("key3");
      keyList.add("key4");
      keyList.add("key5");
      keyList.add("key6");
      keyList.add("key7");
      keyList.add("key8");
      keyList.add("key9");
      keyList.add("key10");


   }

   @BeforeMethod
   public void setUp() throws ParseException {

      // Setting up the cache mock instance
      cache = createMock(Cache.class);

      expect(cache.get(anyObject())).andAnswer(new IAnswer<Person>() {

         public Person answer() throws Throwable {
            String key = getCurrentArguments()[0].toString();
            return dummyDataMap.get(key);
         }
      }).anyTimes();


      // Create mock instances of other things required to create a lazy iterator.

      SearchFactoryImplementor searchFactory = createMock(SearchFactoryImplementor.class);

      DocumentExtractor extractor = org.easymock.classextension.EasyMock.createMock(DocumentExtractor.class);


      try {
         org.easymock.classextension.EasyMock.expect(extractor.extract(anyInt())).andAnswer(new IAnswer<EntityInfo>() {

            public EntityInfo answer() throws Throwable {
               int index = (Integer) getCurrentArguments()[0];
               String keyString = keyList.get(index);

               System.out.println("The key for index parameter " + index + " is " + keyString);

               return new EntityInfo(Person.class, keyString, null);
            }
         }).anyTimes();


      } catch (IOException e) {
         e.printStackTrace();
      }
      IndexSearcher searcher = org.easymock.classextension.EasyMock.createMock(IndexSearcher.class);

      EasyMock.replay(cache, searchFactory);
      org.easymock.classextension.EasyMock.replay(searcher, extractor);

      iterator = new LazyIterator(extractor, cache, searcher, searchFactory, 0, 9, fetchSize);

   }


   @AfterMethod (alwaysRun = true)
   public void tearDown() {
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
   }


   @Test(expectedExceptions = IndexOutOfBoundsException.class)
   public void testOutOfBoundsBelow() {
      iterator.jumpToResult(-1);
   }

   @Test(expectedExceptions = IndexOutOfBoundsException.class)
   public void testOutOfBoundsAbove() {
      iterator.jumpToResult(keyList.size() + 1);
   }


   public void testFirst() {
      assert iterator.isFirst() : "We should be pointing at the first element";
      Object next = iterator.next();

      System.out.println(next);
      assert next == person1;
      assert !iterator.isFirst();

      iterator.first();

      assert iterator.isFirst() : "We should be pointing at the first element";
      next = iterator.next();
      assert next == person1;
      assert !iterator.isFirst();

   }

   public void testLast() {
      //Jumps to the last element
      iterator.last();

      //Makes sure that the iterator is pointing at the last element.
      assert iterator.isLast();

      iterator.first();

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
      assert previous == person2;

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
      assert next == person9;

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
   }

   public void testPreviousAndHasPrevious() {
      iterator.last();

      // This is so that we can "rebuild" the keystring for the nextAndHasNext and the previousAndHasPrevious methods.
      builder = new StringBuilder();

      for (int i = 10; i >= 1; i--) {
         builder.delete(0, 5);      // In this case we know that there are 4 characters in this string. so each time we come into the loop we want to clear the builder.
         builder.append("key");
         builder.append(i);
         String keyString = builder.toString();


         Object expectedValue = cache.get(keyString);

         assert iterator.hasPrevious(); // should have previous as long as we are less than the number of elements.

         Object previous = iterator.previous();

         assert expectedValue == previous; // tests previous()
      }
      assert !iterator.hasPrevious(); // this should now NOT be true.

   }

   public void testNextIndex() {
      iterator.first();
      assert iterator.nextIndex() == 1;

      iterator.last();
      assert iterator.nextIndex() == 10; //Index will be the index of the last element + 1.

   }

   public void testPreviousIndex() {
      iterator.first();
      assert iterator.previousIndex() == -1;

      iterator.last();
      assert iterator.previousIndex() == 8; //Index will be that of the last element - 1.
   }

}


