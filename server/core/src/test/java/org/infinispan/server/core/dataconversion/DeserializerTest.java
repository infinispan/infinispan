package org.infinispan.server.core.dataconversion;

import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.infinispan.server.core.dataconversion.deserializer.Deserializer;
import org.infinispan.server.core.dataconversion.deserializer.SEntity;
import org.testng.annotations.Test;

/**
 * @since 15.0
 **/
@Test(groups = "functional", testName = "server.core.dataconversion.DeserializerTest")
public class DeserializerTest {
   public enum MyEnum {
      A,
      B
   }
   public static class MyPojo implements Serializable {
      String s;
      boolean b;
      short sh;
      int i;
      long l;
      float f;
      double d;
      MyEnum e;
      MyPojo pojo;

   }

   public void testJavaDeserialization() throws IOException {
      MyPojo pojo = new MyPojo();
      pojo.s = "a string";
      pojo.b = true;
      pojo.sh = Short.MAX_VALUE / 2;
      pojo.i = Integer.MAX_VALUE / 2;
      pojo.l = Long.MAX_VALUE / 2;
      pojo.f = 1.234F;
      pojo.d = 3.141;
      pojo.e = MyEnum.A;
      pojo.pojo = new MyPojo();
      pojo.pojo.pojo = pojo; // test recursion

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
         os.writeObject(pojo);
      }

      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      SEntity entity = deserializer.readObject();

      assertEquals("{\"b\":true,\"d\":3.141,\"f\":1.234,\"i\":1073741823,\"l\":4611686018427387903,\"sh\":16383,\"e\":{\"<name>\":\"A\"},\"pojo\":{\"b\":false,\"d\":0.0,\"f\":0.0,\"i\":0,\"l\":0,\"sh\":0,\"e\":null,\"pojo\":{},\"s\":null},\"s\":\"a string\"}", entity.json().toString());
   }
}
