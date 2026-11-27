package org.infinispan.server.core.dataconversion;

import static java.io.ObjectStreamConstants.SC_SERIALIZABLE;
import static java.io.ObjectStreamConstants.SC_WRITE_METHOD;
import static java.io.ObjectStreamConstants.STREAM_MAGIC;
import static java.io.ObjectStreamConstants.STREAM_VERSION;
import static java.io.ObjectStreamConstants.TC_ARRAY;
import static java.io.ObjectStreamConstants.TC_BLOCKDATA;
import static java.io.ObjectStreamConstants.TC_BLOCKDATALONG;
import static java.io.ObjectStreamConstants.TC_CLASSDESC;
import static java.io.ObjectStreamConstants.TC_ENDBLOCKDATA;
import static java.io.ObjectStreamConstants.TC_NULL;
import static java.io.ObjectStreamConstants.TC_OBJECT;
import static java.io.ObjectStreamConstants.TC_REFERENCE;
import static java.io.ObjectStreamConstants.TC_RESET;
import static java.io.ObjectStreamConstants.TC_STRING;
import static java.io.ObjectStreamConstants.baseWireHandle;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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

   /**
    * writeObject() that does not call defaultWriteObject(), like Jetty's SessionData: no field values are present in the stream at all
    */
   public static class NoDefaultWriteObject implements Serializable {
      int x;
      String y;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeInt(x);
         out.write(new byte[]{(byte) 0xCA, (byte) 0xFE});
      }
   }

   /**
    * writeObject() that calls defaultWriteObject() and then writes extra custom data
    */
   public static class WithDefaultWriteObject implements Serializable {
      int x;
      String y;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();
         out.writeInt(42);
      }
   }

   /**
    * writeObject() that writes custom data before calling defaultWriteObject(): the section opens with the custom
    * data (a block-data record) followed by the raw field values
    */
   public static class CustomBeforeDefault implements Serializable {
      int x;
      String y;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeInt(7);
         out.defaultWriteObject();
         out.write(new byte[]{(byte) 0xCA, (byte) 0xFE});
      }
   }

   /**
    * writeObject() that writes custom data before calling defaultWriteObject() where the custom data also happens
    * to parse as the declared fields: the leading bytes are TC_NULLs and the raw field value of x is chosen so its
    * bytes are TC_NULLs too, letting a misaligned parse consume the custom data as the fields
    */
   public static class CustomBeforeDefaultParseable implements Serializable {
      int x;
      String y;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.write(new byte[]{0x70, 0x70});
         out.writeObject("junk");
         out.defaultWriteObject();
      }
   }

   /**
    * base class with a custom writeObject() that skips defaultWriteObject(), derived adds plain fields
    */
   public static class BaseNoDefault implements Serializable {
      int x;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeInt(x);
      }
   }

   public static class DerivedNoDefault extends BaseNoDefault {
      String y;
   }

   /**
    * both classes declare a custom writeObject() that calls defaultWriteObject() and writes extra data
    */
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

   /**
    * first declared field is a reference type positioned where blockdata starts
    */
   public static class NoDefaultRefFirst implements Serializable {
      String s;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.write(new byte[]{(byte) 0xCA, (byte) 0xFE});
      }
   }

   /**
    * two primitive fields: the first one is filled with garbage before parsing fails on the second
    */
   public static class NoDefaultPrimitiveFirst implements Serializable {
      int a;
      int b;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeInt(42);
      }
   }

   /**
    * writeObject() that skips defaultWriteObject() and writes a nested object with its own writeObject() (an ArrayList) before primitive data: the nested custom writer creates an inner mark while the outer section is being parsed
    */
   public static class NestedCustom implements Serializable {
      Object a;
      String b;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeObject(new ArrayList<>(List.of("x")));
         out.writeInt(7);
      }
   }

   /**
    * a no-default writer nested inside another no-default writer: exercises the rewind through two levels of nested marks
    */
   public static class OuterNestedCustom implements Serializable {
      Object a;
      String b;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeObject(new NestedCustom());
         out.writeInt(7);
      }
   }

   /**
    * the inner no-default writer's section is short: its speculative field parse reads past it and exhausts the outer replay before failing
    */
   public static class InnerNoDefault implements Serializable {
      long a, b, c, d;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeInt(42);
      }
   }

   /**
    * a no-default writer containing a nested no-default writer followed by trailing data: the inner mark is created while the outer replay is being served, and the inner field parse drains the replay before failing
    */
   public static class OuterNoDefaultWithTrailer implements Serializable {
      Object a, b;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeObject(new InnerNoDefault());
         out.writeInt(7);
      }
   }

   /**
    * the outer writer calls defaultWriteObject() and nests a no-default writer in its custom data: the inner slot is rewound and re-parsed as custom data while the outer section completes normally
    */
   public static class OuterSuccessInnerNoDefault implements Serializable {
      Object a;
      String b;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();
         out.writeObject(new NoDefaultWriteObject());
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
      PlainPojo followUp = new PlainPojo();
      followUp.s = "after";

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
         os.writeObject(obj);
         os.writeObject(followUp);
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertEquals("{}", deserializer.readObject().json().toString());
      // the custom-data section must have been consumed along with the empty object: reading one object only would
      // not detect a reader that stops before the section, leaving its bytes to corrupt the next read
      assertEquals("{\"s\":\"after\"}", deserializer.readObject().json().toString());
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

   /**
    * Custom data written before defaultWriteObject() must not shadow the default field values: the section opens
    * with a block-data record followed by the raw values. Before the fix the speculative parse consumed the record's
    * framing bytes as the fields, failed on the real values, and the whole section was then rejected as malformed.
    */
   public void testCustomDataBeforeDefaultWriteObject() throws IOException {
      CustomBeforeDefault obj = new CustomBeforeDefault();
      obj.x = 42;
      obj.y = "leaf";

      Json json = deserialize(obj).json();

      assertEquals(42, json.at("x").asInteger());
      assertEquals("leaf", json.at("y").asString());
   }

   /**
    * Custom data written before defaultWriteObject() that also parses as the declared fields must not be presented
    * as the field values: before the fix the speculative parse consumed it as the fields and discarded the real
    * values as annotations, so the conversion silently returned the custom data as the fields.
    */
   public void testCustomDataBeforeDefaultWriteObjectParsedAsFields() throws IOException {
      CustomBeforeDefaultParseable obj = new CustomBeforeDefaultParseable();
      obj.x = 0x70707070; // its raw bytes are TC_NULLs, so the custom data parses as the fields
      obj.y = "real";

      Json json = deserialize(obj).json();

      assertEquals(0x70707070, json.at("x").asInteger());
      assertEquals("real", json.at("y").asString());
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

   public void testWriteObjectWithoutDefaultWithNestedCustomObject() throws IOException {
      SEntity entity = deserialize(new NestedCustom());

      // the nested ArrayList's custom writer creates an inner mark; the outer rewind must still reach the start of the outer section
      assertEquals("{}", entity.json().toString());
   }

   public void testWriteObjectWithoutDefaultWithNestedNoDefaultObject() throws IOException {
      SEntity entity = deserialize(new OuterNestedCustom());

      // two levels of nested custom writers: the rewind must unwind both marks
      assertEquals("{}", entity.json().toString());
   }

   public void testOuterSuccessInnerNoDefault() throws IOException {
      SEntity entity = deserialize(new OuterSuccessInnerNoDefault());

      // the inner no-default writer is re-parsed as custom data; the outer fields come from defaultWriteObject() and the outer mark is released without a rewind
      assertEquals("{\"a\":null,\"b\":null}", entity.json().toString());
   }

   public void testOuterRewindWithInnerMarkDuringReplay() throws IOException {
      SEntity entity = deserialize(new OuterNoDefaultWithTrailer());

      // the inner mark is created while the outer replay is being served and its field parse exhausts the replay before failing: the inner rewind must splice the replay tail with the capture
      assertEquals("{}", entity.json().toString());
   }

   /**
    * Exercises the handle-table and class-descriptor rollback: a failed speculative parse reads an actual TC_RESET
    * (moving the logical base of the handle table) and an actual class descriptor spelling an existing class with a
    * different field type (int instead of String), mutating the table entry. The rollback must restore both: the
    * subsequent object references the class through its pre-section handle, so a stale reset breaks the handle
    * resolution and an unrestored descriptor parses the String field as an int. The stream is hand-written because
    * ObjectOutputStream encodes writes inside a custom writeObject() as block-data payload, so the control tokens
    * would never be parsed as control tokens at all.
    */
   public void testRewindRestoresStateAfterTCReset() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         // registers Mutated's descriptor, with its String field, as the first handle
         writeStringFieldObject(out, "Mutated", "v1");
         // a custom writer whose failed speculative parse reads the TC_RESET and the fake descriptor, both hidden
         // in a block-data record: see writeDescriptorMutatingCustomWriter
         writeDescriptorMutatingCustomWriter(out, "Mutated");
         // a subsequent object of the same class, through the descriptor's handle
         out.write(TC_OBJECT);
         out.write(TC_REFERENCE);
         out.writeInt(baseWireHandle);
         out.write(TC_STRING);
         out.writeUTF("v3");
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertEquals("{\"s\":\"v1\"}", deserializer.readObject().json().toString());
      assertEquals("{}", deserializer.readObject().json().toString());
      // the field must render as a string: a descriptor left with the mutated int layout would read it as an int,
      // and a handle table left with the reset's base would fail to resolve the reference at all
      assertEquals("{\"s\":\"v3\"}", deserializer.readObject().json().toString());
   }

   public static class PlainPojo implements Serializable {
      String s;
   }

   public void testArrayDeserialization() throws IOException {
      assertEquals("[1,2,3]", deserialize(new byte[]{1, 2, 3}).json().toString());
      assertEquals("[\"a\",\"b\"]", deserialize(new String[]{"a", "b"}).json().toString());

      // an array longer than the backing store's initial capacity: the store grows to the declared size as the
      // elements are read and renders all of them
      int n = 1000;
      int[] ints = new int[n];
      for (int i = 0; i < n; i++) ints[i] = i;
      Json json = deserialize(ints).json();
      assertEquals(n, json.asJsonList().size());
      for (int i = 0; i < n; i++) assertEquals(i, json.at(i).asInteger());
   }

   public void testBlockDataRendersLength() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         out.write(TC_BLOCKDATA);
         out.write(3);
         out.write(new byte[]{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA});
         // a long-form block longer than a single read chunk, so counting it off spans several reads
         out.write(TC_BLOCKDATALONG);
         out.writeInt(20000);
         out.write(new byte[20000]);
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertEquals("\"byte[3]\"", deserializer.readObject().json().toString());
      assertEquals("\"byte[20000]\"", deserializer.readObject().json().toString());
   }

   /**
    * A declared array size within the accepted limits must be backed by an actual payload: a stream that declares a
    * huge array whose elements are absent or truncated is rejected as corrupt instead of the declared size being
    * allocated upfront. Before the fix the declared size alone drove the allocation, exhausting the server heap.
    */
   public void testArrayWithDeclaredSizeWithinLimitsButNoPayloadIsRejected() throws IOException {
      // 1 << 29 is the largest size the deserializer accepts: within the limits, yet unbacked by any payload
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(streamDeclaringByteArrayOfSize(1 << 29)), true);
      assertThrows(StreamCorruptedException.class, deserializer::readObject);

      // a payload that stops short of the declared size is rejected the same way
      deserializer = new Deserializer(new ByteArrayInputStream(streamDeclaringByteArrayOfSize(4, (byte) 0xCA)), true);
      assertThrows(StreamCorruptedException.class, deserializer::readObject);
   }

   /**
    * A custom-writer section where the declared fields parse at several positions before the terminator is
    * ambiguous: no field layout can be presented as the correct one, so the section renders as custom data only.
    * Before the fix the first speculative parse was presented as the field values.
    */
   public void testAmbiguousCustomWriterSectionRendersCustomDataOnly() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         out.write(TC_OBJECT);
         out.write(TC_CLASSDESC);
         out.writeUTF("Ambiguous");
         out.writeLong(0L);
         out.write(SC_SERIALIZABLE | SC_WRITE_METHOD);
         out.writeShort(1);
         out.write('I');
         out.writeUTF("x");
         out.write(TC_ENDBLOCKDATA); // empty class annotation
         out.write(TC_NULL); // no superclass
         // one block-data record followed by raw TC_NULL bytes chosen so that the int field parses at several
         // offsets, each reaching the terminator: no single layout is presentable
         out.write(TC_BLOCKDATA);
         out.write(2);
         out.write(new byte[]{0x70, 0x70});
         out.write(new byte[]{0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70, 0x70});
         out.write(TC_ENDBLOCKDATA);
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertEquals("{}", deserializer.readObject().json().toString());
   }

   /**
    * A TC_RESET read by a failed speculative attempt must not discard the handles registered before the section:
    * the reset moves the logical base of the handle table and the rollback restores it, so a later TC_REFERENCE to
    * an earlier handle still resolves. The stream below hides TC_RESET bytes in a block-data record's payload: the
    * fields-first interpretation reads them as type codes (and fails), while the winning interpretation consumes the
    * record opaquely and reads the real field values after it.
    */
   public void testResetInFailedAttemptDoesNotDiscardEarlierHandles() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         writeStringFieldObject(out, "P", "v1"); // registers P's descriptor as the first handle
         // a custom writer with an int and a reference field, whose section opens with a block-data record whose
         // payload is TC_RESET bytes
         out.write(TC_OBJECT);
         out.write(TC_CLASSDESC);
         out.writeUTF("ResettingWriter");
         out.writeLong(0L);
         out.write(SC_SERIALIZABLE | SC_WRITE_METHOD);
         out.writeShort(2);
         out.write('I');
         out.writeUTF("x");
         writeReferenceField(out, "f");
         out.write(TC_ENDBLOCKDATA); // empty class annotation
         out.write(TC_NULL); // no superclass
         out.write(TC_BLOCKDATA);
         out.write(4);
         out.write(new byte[]{0x79, 0x79, 0x79, 0x79}); // TC_RESET bytes, opaque to the winning layout
         out.writeInt(7); // the real value of x, raw
         out.write(TC_NULL); // the real value of f
         out.write(TC_ENDBLOCKDATA);
         // reference P's descriptor through its handle: the failed attempt's resets must have been rolled back
         out.write(TC_OBJECT);
         out.write(TC_REFERENCE);
         out.writeInt(baseWireHandle);
         out.write(TC_STRING);
         out.writeUTF("v3");
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertEquals("{\"s\":\"v1\"}", deserializer.readObject().json().toString());
      assertEquals("{\"x\":7,\"f\":null}", deserializer.readObject().json().toString());
      assertEquals("{\"s\":\"v3\"}", deserializer.readObject().json().toString());
   }

   /**
    * A stream with many custom-writer sections must convert in time proportional to the stream: the rollback state
    * used to be snapshotted by copying the whole handle list and descriptor table for every section, including the
    * entries accumulated from earlier objects, which made conversion quadratic in the number of sections.
    */
   public void testManyCustomWriterSectionsConvertInLinearTime() throws IOException {
      int count = 100_000;
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
         for (int i = 0; i < count; i++) {
            WithDefaultWriteObject obj = new WithDefaultWriteObject();
            obj.x = 7;
            obj.y = "v" + i; // a unique value per object, so the handle table grows with the stream
            os.writeObject(obj);
         }
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
         for (int i = 0; i < count; i++) {
            SEntity entity = deserializer.readObject();
            if (i == count - 1) {
               Json json = entity.json();
               assertEquals(7, json.at("x").asInteger());
               assertEquals("v" + (count - 1), json.at("y").asString());
            }
         }
      });
   }

   /**
    * A declared block-data length within the accepted limits must be backed by an actual payload: a stream that
    * declares a huge block with a truncated payload is rejected as corrupt instead of the declared length being
    * buffered upfront. Before the fix the declared length alone drove the allocation, exhausting the server heap.
    */
   public void testBlockDataWithDeclaredLengthWithinLimitsButTruncatedPayloadIsRejected() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         out.write(TC_BLOCKDATALONG);
         out.writeInt(1 << 30); // the largest length the deserializer accepts
         out.write(new byte[]{(byte) 0xCA, (byte) 0xFE}); // only 2 of the declared bytes are present
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertThrows(StreamCorruptedException.class, deserializer::readObject);
   }

   /**
    * An array descriptor must be restored by the rollback as an array descriptor: the snapshot used to create plain
    * descriptors for array descriptors, so re-declaring the same array type by name miscast the restored table entry
    * in getArrayClassDesc and failed with a ClassCastException.
    */
   public void testRollbackPreservesArrayDescriptors() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         writeByteArrayOfSize(out, 1, (byte) 5); // registers the [B array descriptor
         writeBlockDataRejectingCustomWriter(out); // forces a rollback that rebuilds the descriptor table
         writeByteArrayOfSize(out, 1, (byte) 6); // re-declares [B by name
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertEquals("[5]", deserializer.readObject().json().toString());
      assertEquals("{}", deserializer.readObject().json().toString());
      assertEquals("[6]", deserializer.readObject().json().toString());
   }

   /**
    * After a rollback the handle table must reference the descriptor copies: it used to keep the original
    * descriptors, so a later object referencing the class through TC_REFERENCE deserialized with the field layout the
    * failed parse had mutated in (an int where a String was declared).
    */
   public void testRollbackRestoresHandleReferencesToDescriptorCopies() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         writeStringFieldObject(out, "P", "v1"); // registers P's descriptor as the first handle
         writeDescriptorMutatingCustomWriter(out, "P"); // mutates P's descriptor, then rolls back
         out.write(TC_OBJECT);
         out.write(TC_REFERENCE);
         out.writeInt(baseWireHandle); // reference P's descriptor through its handle
         out.write(TC_STRING);
         out.writeUTF("v3");
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertEquals("{\"s\":\"v1\"}", deserializer.readObject().json().toString());
      assertEquals("{}", deserializer.readObject().json().toString());
      assertEquals("{\"s\":\"v3\"}", deserializer.readObject().json().toString());
   }

   /**
    * After a rollback the cached hierarchy of a restored descriptor must reference the copies: the snapshot used to
    * copy the hierarchy list as-is, so a later object of the same class — even re-declared by name and parsed through
    * the restored table entry — read its fields from the mutated original descriptor.
    */
   public void testRollbackRemapsCachedHierarchiesToDescriptorCopies() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         writeStringFieldObject(out, "P", "v1");
         writeDescriptorMutatingCustomWriter(out, "P");
         writeStringFieldObject(out, "P", "v3"); // re-declared by name
      }
      Deserializer deserializer = new Deserializer(new ByteArrayInputStream(baos.toByteArray()), true);
      assertEquals("{\"s\":\"v1\"}", deserializer.readObject().json().toString());
      assertEquals("{}", deserializer.readObject().json().toString());
      assertEquals("{\"s\":\"v3\"}", deserializer.readObject().json().toString());
   }

   private static void writeStreamHeader(DataOutputStream out) throws IOException {
      // the constants are signed shorts: write their unsigned 16-bit form
      out.writeShort(STREAM_MAGIC & 0xFFFF);
      out.writeShort(STREAM_VERSION & 0xFFFF);
   }

   // writes a TC_OBJECT followed by a class descriptor for the named class declaring a single String field "s",
   // then the field's value as a TC_STRING
   private static void writeStringFieldObject(DataOutputStream out, String name, String value) throws IOException {
      out.write(TC_OBJECT);
      out.write(TC_CLASSDESC);
      out.writeUTF(name);
      out.writeLong(0L); // serialVersionUID, not validated by the structural parser
      out.write(SC_SERIALIZABLE);
      out.writeShort(1);
      writeReferenceField(out, "s");
      out.write(TC_ENDBLOCKDATA); // empty class annotation
      out.write(TC_NULL); // no superclass
      out.write(TC_STRING);
      out.writeUTF(value);
   }

   /**
    * Writes a custom-writer object with a single reference field whose section is one block-data record: the
    * speculative parse of the section as field values rejects the block data, forcing a rollback, and the custom
    * re-read consumes the record opaquely.
    */
   private static void writeBlockDataRejectingCustomWriter(DataOutputStream out) throws IOException {
      out.write(TC_OBJECT);
      out.write(TC_CLASSDESC);
      out.writeUTF("BlockDataWriter");
      out.writeLong(0L);
      out.write(SC_SERIALIZABLE | SC_WRITE_METHOD);
      out.writeShort(1);
      writeReferenceField(out, "f");
      out.write(TC_ENDBLOCKDATA); // empty class annotation
      out.write(TC_NULL); // no superclass
      out.write(TC_BLOCKDATA);
      out.write(1);
      out.write(0xFF);
      out.write(TC_ENDBLOCKDATA);
   }

   /**
    * Writes a custom-writer object whose speculative field parse reads an actual TC_RESET and mutates the descriptor
    * of the named class: the section's single block-data record hides a TC_RESET followed by a class descriptor
    * spelling that class with an int field where the real class declares a String field. The leading int field
    * consumes the record header and two padding bytes, so the speculative parse continues inside the payload and
    * reads the reset (moving the logical base of the handle table) and the embedded descriptor (mutating the
    * existing table entry), and then fails on an invalid type code. The custom re-read consumes the record opaquely,
    * so neither is ever repeated.
    */
   private static void writeDescriptorMutatingCustomWriter(DataOutputStream out, String target) throws IOException {
      out.write(TC_OBJECT);
      out.write(TC_CLASSDESC);
      out.writeUTF("Mutator");
      out.writeLong(0L);
      out.write(SC_SERIALIZABLE | SC_WRITE_METHOD);
      out.writeShort(2);
      out.write('I');
      out.writeUTF("x"); // consumes the block-data header and the padding when read as an int
      writeReferenceField(out, "f"); // reads the embedded reset and class descriptor while looking for its value
      out.write(TC_ENDBLOCKDATA); // empty class annotation
      out.write(TC_NULL); // no superclass
      ByteArrayOutputStream payload = new ByteArrayOutputStream();
      try (DataOutputStream p = new DataOutputStream(payload)) {
         p.write(0xCA);
         p.write(0xFE);
         p.write(TC_RESET);
         p.write(TC_CLASSDESC);
         p.writeUTF(target);
         p.writeLong(0L);
         p.write(SC_SERIALIZABLE);
         p.writeShort(1);
         p.write('I');
         p.writeUTF("s");
         p.write(TC_ENDBLOCKDATA); // empty class annotation
         p.write(TC_NULL); // no superclass
         p.write(0xFF); // invalid type code: fails the speculative parse after the reset and the mutation
      }
      byte[] record = payload.toByteArray();
      out.write(TC_BLOCKDATA);
      out.write(record.length);
      out.write(record);
      out.write(TC_ENDBLOCKDATA);
   }

   // writes a reference field declaration: 'L', the field name, and its type spelled as a string
   private static void writeReferenceField(DataOutputStream out, String name) throws IOException {
      out.write('L');
      out.writeUTF(name);
      out.write(TC_STRING);
      out.writeUTF("Ljava/lang/String;");
   }

   /**
    * Builds a serialized stream that declares a byte[] of the given size followed by the given element bytes: the
    * declared size can exceed the elements actually written, mimicking a forged or truncated stream.
    */
   static byte[] streamDeclaringByteArrayOfSize(int declaredSize, byte... elements) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(baos)) {
         writeStreamHeader(out);
         writeByteArrayOfSize(out, declaredSize, elements);
      }
      return baos.toByteArray();
   }

   // writes a TC_ARRAY followed by a class descriptor for byte[] of the given size and the given element bytes
   private static void writeByteArrayOfSize(DataOutputStream out, int declaredSize, byte... elements) throws IOException {
      out.write(TC_ARRAY);
      out.write(TC_CLASSDESC);
      out.writeUTF("[B");
      out.writeLong(0L); // serialVersionUID, not validated by the structural parser
      out.write(SC_SERIALIZABLE);
      out.writeShort(0); // no fields
      out.write(TC_ENDBLOCKDATA); // empty class annotation
      out.write(TC_NULL); // no superclass
      out.writeInt(declaredSize);
      out.write(elements);
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
