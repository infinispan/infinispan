package org.infinispan.jboss.marshalling;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.commons.marshall.NotSerializableException;
import org.testng.annotations.Test;

public class VersionAwareMarshallerTest extends org.infinispan.marshall.VersionAwareMarshallerTest {

   public void testPojoWithJBossMarshallingExternalizer(Method m) throws Exception {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(27, k(m));
      marshallAndAssertEquality(pojo);
   }

   public void testIsMarshallableJBossExternalizeAnnotation() throws Exception {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(34, "k2");
      assertTrue(marshaller.isMarshallable(pojo));
   }

   @Override
   @Test(expectedExceptions = NotSerializableException.class)
   public void testNestedNonSerializable() throws Exception {
      super.testNestedNonSerializable();
   }

   @Override
   @Test(expectedExceptions = NotSerializableException.class)
   public void testNonSerializable() throws Exception {
      super.testNonSerializable();
   }
}
