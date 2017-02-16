package org.infinispan.marshaller.test;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(groups = "functional")
public abstract class AbstractMarshallingTest extends AbstractInfinispanTest {

   private final List<TestObject> testObjects = IntStream.range(1, 2).boxed().map(this::getTestObject).collect(Collectors.toList());
   private final Marshaller marshaller;
   private ByteArrayOutputStream outputStream;

   protected AbstractMarshallingTest(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   @BeforeMethod(alwaysRun = true)
   protected void init() {
      outputStream = new ByteArrayOutputStream(1024);
   }

   @AfterMethod(alwaysRun = true)
   protected void reset() throws Exception {
      resetCustomerSerializerCounters();
      outputStream.close();
   }

   abstract protected void checkCustomSerializerCounters(int readCount, int writeCount);
   abstract protected void resetCustomerSerializerCounters();

   protected TestObject getTestObject(int id) {
      TestObject testObject = new TestObject(id);
      Map<Integer, String> map = new HashMap<>();
      map.put(id, "Test" + id);
      testObject.setMap(map);
      testObject.setList(IntStream.range(0, id).boxed().collect(Collectors.toList()));
      testObject.setUser(new User("User" + id));
      return testObject;
   }

   @Test
   protected void testObjectMarshallingTest() throws Exception {
      List<ByteBuffer> serializedObjects = new ArrayList<>(testObjects.size());

      for (TestObject object : testObjects) {
         serializedObjects.add(marshaller.objectToBuffer(object));
      }

      assert serializedObjects.size() == testObjects.size();
      for (int i = 0; i < testObjects.size(); i++) {
         byte[] bytes = serializedObjects.get(i).getBuf();
         Object testObj = testObjects.get(i);
         Object unmarshalledObj = marshaller.objectFromByteBuffer(bytes);
         assert testObj.equals(unmarshalledObj);
      }
   }

   @Test
   protected void testRegisterSerializersAreUtilised() throws Exception {
      TestObject obj = testObjects.get(0);
      byte[] bytes = marshaller.objectToByteBuffer(obj);
      assert marshaller.objectFromByteBuffer(bytes).equals(obj);
      checkCustomSerializerCounters(1, 1);
   }

   @Test
   protected void testImmutableCollections() throws Exception {
      int listSize = 10;
      TestObject obj = testObjects.get(0);
      obj.setList(Collections.unmodifiableList(IntStream.range(0, 10).boxed().collect(Collectors.toList())));
      byte[] bytes = marshaller.objectToByteBuffer(obj);
      TestObject unmarshalledObj =  (TestObject) marshaller.objectFromByteBuffer(bytes);
      assert unmarshalledObj.getList().size() == listSize;
      for (int i = 0; i < listSize; i++)
         assert unmarshalledObj.getList().get(i) == i;
   }

   @Test
   protected void testSerializingByteArrays() throws Exception {
      byte[] bytes = new byte[10];
      IntStream.range(0, 10).forEach(i -> bytes[i] = (byte) i);
      byte[] marshalledBytes = marshaller.objectToBuffer(bytes).getBuf();
      byte[] unmarsalledBytes = (byte[]) marshaller.objectFromByteBuffer(marshalledBytes);
      IntStream.range(0, 10).forEach(i -> assertEquals(unmarsalledBytes[i], i));
   }
}
