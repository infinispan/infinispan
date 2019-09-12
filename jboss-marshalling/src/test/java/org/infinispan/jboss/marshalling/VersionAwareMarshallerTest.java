package org.infinispan.jboss.marshalling;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.NotSerializableException;
import org.jboss.marshalling.TraceInformation;
import org.testng.annotations.Test;

public class VersionAwareMarshallerTest extends org.infinispan.marshall.VersionAwareMarshallerTest {

   @Override
   @Test(expectedExceptions = NotSerializableException.class)
   public void testNestedNonMarshallable() throws Exception {
      super.testNestedNonMarshallable();
   }

   @Override
   @Test(expectedExceptions = NotSerializableException.class)
   public void testNonMarshallable() throws Exception {
      super.testNonMarshallable();
   }

   public void testPojoWithJBossMarshallingExternalizer(Method m) throws Exception {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(27, k(m));
      marshallAndAssertEquality(pojo);
   }

   public void testIsMarshallableJBossExternalizeAnnotation() throws Exception {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(34, "k2");
      assertTrue(marshaller.isMarshallable(pojo));
   }

   public void testMarshallObjectThatContainsACustomReadObjectMethod() throws Exception {
      JBossMarshallingTest.ObjectThatContainsACustomReadObjectMethod obj = new JBossMarshallingTest.ObjectThatContainsACustomReadObjectMethod();
      obj.anObjectWithCustomReadObjectMethod = new JBossMarshallingTest.CustomReadObjectMethod();
      marshallAndAssertEquality(obj);
   }

   public void testMarshallingNestedSerializableSubclass() throws Exception {
      Child1 child1Obj = new Child1(1234, "1234");
      Child2 child2Obj = new Child2(2345, "2345", child1Obj);
      byte[] bytes = marshaller.objectToByteBuffer(child2Obj);
      Child2 readChild2 = (Child2) marshaller.objectFromByteBuffer(bytes);
      assertEquals(2345, readChild2.someInt);
      assertEquals("2345", readChild2.getId());
      assertEquals(1234, readChild2.getChild1Obj().someInt);
      assertEquals("1234", readChild2.getChild1Obj().getId());
   }

   public void testMarshallingSerializableSubclass() throws Exception {
      Child1 child1Obj = new Child1(1234, "1234");
      byte[] bytes = marshaller.objectToByteBuffer(child1Obj);
      Child1 readChild1 = (Child1) marshaller.objectFromByteBuffer(bytes);
      assertEquals(1234, readChild1.someInt);
      assertEquals("1234", readChild1.getId());
   }

   public void testTreeSetWithComparator() throws Exception {
      Set<Human> treeSet = new TreeSet<>(new HumanComparator());
      for (int i = 0; i < 10; i++) {
         treeSet.add(new Human().age(i));
      }
      marshallAndAssertEquality(treeSet);
   }

   @Override
   public void testErrorUnmarshalling() throws Exception {
      Pojo pojo = new PojoWhichFailsOnUnmarshalling();
      byte[] bytes = marshaller.objectToByteBuffer(pojo);
      try {
         marshaller.objectFromByteBuffer(bytes);
      } catch (MarshallingException e) {
         IOException ioException = (IOException) e.getCause();
         TraceInformation inf = (TraceInformation) ioException.getCause();
         assert inf.toString().contains("in object of type org.infinispan.marshall.VersionAwareMarshallerTest$PojoWhichFailsOnUnmarshalling");
      }
   }

   static class Parent implements Serializable {
      private final String id;
      private final Child1 child1Obj;

      public Parent(String id, Child1 child1Obj) {
         this.id = id;
         this.child1Obj = child1Obj;
      }

      public String getId() {
         return id;
      }
      public Child1 getChild1Obj() {
         return child1Obj;
      }
   }

   static class Child1 extends Parent {
      private final int someInt;

      public Child1(int someInt, String parentStr) {
         super(parentStr, null);
         this.someInt = someInt;
      }

   }

   static class Child2 extends Parent {
      private final int someInt;

      public Child2(int someInt, String parentStr, Child1 child1Obj) {
         super(parentStr, child1Obj);
         this.someInt = someInt;
      }
   }
}
