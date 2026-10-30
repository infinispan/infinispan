package org.infinispan.server.core.dataconversion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.infinispan.commons.dataconversion.internal.Json;
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

   /** writeObject() that does not call defaultWriteObject(), like Jetty's SessionData: no field values are present in the stream at all */
   public static class NoDefaultWriteObject implements Serializable {
      int x;
      String y;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeInt(x);
         out.write(new byte[]{(byte) 0xCA, (byte) 0xFE});
      }
   }

   /** writeObject() that calls defaultWriteObject() and then writes extra custom data */
   public static class WithDefaultWriteObject implements Serializable {
      int x;
      String y;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();
         out.writeInt(42);
      }
   }

   /** base class with a custom writeObject() that skips defaultWriteObject(), derived adds plain fields */
   public static class BaseNoDefault implements Serializable {
      int x;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeInt(x);
      }
   }

   public static class DerivedNoDefault extends BaseNoDefault {
      String y;
   }

   /** both classes declare a custom writeObject() that calls defaultWriteObject() and writes extra data */
   public static class BaseBothCustom implements Serializable {
      int x;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();
         out.writeInt(0xBEEF);
      }
   }

   public static class DerivedBothCustom extends BaseBothCustom {
      String y;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();
         out.write(new byte[]{(byte) 0xCA, (byte) 0xFE});
      }
   }

   /** first declared field is a reference type positioned where blockdata starts */
   public static class NoDefaultRefFirst implements Serializable {
      String s;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.write(new byte[]{(byte) 0xCA, (byte) 0xFE});
      }
   }

   /** two primitive fields: the first one is filled with garbage before parsing fails on the second */
   public static class NoDefaultPrimitiveFirst implements Serializable {
      int a;
      int b;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeInt(42);
      }
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

   public void testWriteObjectWithoutDefault() throws IOException {
      NoDefaultWriteObject obj = new NoDefaultWriteObject();
      // low byte 2A is not a valid type code, so parsing the custom data as fields fails deterministically no matter which field comes first in the descriptor
      obj.x = 10842;
      obj.y = "leaf";

      SEntity entity = deserialize(obj);

      assertEquals("{}", entity.json().toString());
   }

   public void testWriteObjectWithDefaultAndCustomData() throws IOException {
      WithDefaultWriteObject obj = new WithDefaultWriteObject();
      obj.x = 7;
      obj.y = "leaf";

      SEntity entity = deserialize(obj);

      Json json = entity.json();
      assertEquals(7, json.at("x").asInteger());
      assertEquals("leaf", json.at("y").asString());
   }

   public void testInheritedWriteObjectWithoutDefault() throws IOException {
      DerivedNoDefault obj = new DerivedNoDefault();
      obj.x = 10842;
      obj.y = "leaf";

      SEntity entity = deserialize(obj);

      // the base section contains only custom data: its field is not rendered, but parsing must not fail
      assertEquals("{\"y\":\"leaf\"}", entity.json().toString());
   }

   public void testBothClassesWithCustomWriteObject() throws IOException {
      DerivedBothCustom obj = new DerivedBothCustom();
      obj.x = 7;
      obj.y = "leaf";

      SEntity entity = deserialize(obj);

      Json json = entity.json();
      assertEquals(7, json.at("x").asInteger());
      assertEquals("leaf", json.at("y").asString());
   }

   public void testWriteObjectWithoutDefaultReferenceFieldFirst() throws IOException {
      NoDefaultRefFirst obj = new NoDefaultRefFirst();

      SEntity entity = deserialize(obj);

      // blockdata must not be silently accepted as a field value: the section is re-parsed as custom data only
      assertEquals("{}", entity.json().toString());
   }

   public void testWriteObjectWithoutDefaultPrimitiveFields() throws IOException {
      NoDefaultPrimitiveFirst obj = new NoDefaultPrimitiveFirst();

      SEntity entity = deserialize(obj);

      // the first field is filled with garbage bytes before parsing fails: it must be rolled back on rewind
      assertEquals("{}", entity.json().toString());
   }

   private static SEntity deserialize(Object obj) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
         os.writeObject(obj);
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      return deserializer.readObject();
   }
}
