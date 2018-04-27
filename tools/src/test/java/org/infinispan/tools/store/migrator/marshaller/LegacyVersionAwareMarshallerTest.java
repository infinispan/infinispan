package org.infinispan.tools.store.migrator.marshaller;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.test.data.Key;
import org.infinispan.test.data.Person;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests to ensure that LegacyVersionAwareMarshaller can correctly unmarshall infinispan 8.x bytes. Note, instructions
 * on how to generate the bin file used in this test are found in the comments at the bottom of the test.
 */
@Test(testName = "tools.LegacyVersionAwareMarshallerTest", groups = "functional")
public class LegacyVersionAwareMarshallerTest {

   private final StreamingMarshaller marshaller;
   private Map<String, byte[]> byteMap;

   public LegacyVersionAwareMarshallerTest() {
      Map<Integer, AdvancedExternalizer<?>> externalizerMap = new HashMap<>();
      externalizerMap.put(256, new TestObjectExternalizer());
      marshaller = new LegacyVersionAwareMarshaller(externalizerMap);
   }

   @BeforeClass(alwaysRun = true)
   public void beforeTest() throws Exception {
      Path path = new File("src/test/resources/marshalled_bytes_8.x.bin").toPath();
      byte[] bytes = Files.readAllBytes(path);
      byteMap = (Map<String, byte[]>) marshaller.objectFromByteBuffer(bytes);
   }

   public void testUnmarshalling() throws Exception {
      unmarshallAndAssertEquality("List", Arrays.asList(new Person("Alan Shearer"), new Person("Nolberto Solano")));
      unmarshallAndAssertEquality("SingletonList", Collections.singletonList(new Key("Key", false)));
      unmarshallAndAssertEquality("SingletonMap", Collections.singletonMap("Key", "Value"));
      unmarshallAndAssertEquality("SingletonSet", Collections.singleton(new Key("Key", false)));
      unmarshallAndAssertEquality("KeyValuePair", new KeyValuePair<>("Key", "Value"));
      unmarshallAndAssertEquality("ImmortalCacheEntry", new ImmortalCacheEntry("Key", "Value"));
      unmarshallAndAssertEquality("MortalCacheEntry", new MortalCacheEntry("Key", "Value", 1, 1));
      unmarshallAndAssertEquality("TransientCacheEntry", new TransientCacheEntry("Key", "Value", 1, 1));
      unmarshallAndAssertEquality("TransientMortalCacheEntry", new TransientMortalCacheEntry("Key", "Value", 1, 1, 1));
      unmarshallAndAssertEquality("ImmortalCacheValue", new ImmortalCacheValue("Value"));
      unmarshallAndAssertEquality("MortalCacheValue", new MortalCacheValue("Value", 1, 1));
      unmarshallAndAssertEquality("TransientCacheValue", new TransientCacheValue("Value", 1, 1));
      unmarshallAndAssertEquality("TransientMortalCacheValue", new TransientMortalCacheValue("Value", 1, 1, 1));

      Metadata metadata = new EmbeddedMetadata.Builder().version(new NumericVersion(1)).build();
      unmarshallAndAssertEquality("EmbeddedMetadata", metadata);

      unmarshallAndAssertEquality("SimpleClusteredVersion", new SimpleClusteredVersion(1, 1));
      unmarshallAndAssertEquality("MetadataImmortalCacheEntry", new MetadataImmortalCacheEntry("Key", "Value", metadata));
      unmarshallAndAssertEquality("MetadataMortalCacheEntry", new MetadataMortalCacheEntry("Key", "Value", metadata, 1));
      unmarshallAndAssertEquality("MetadataTransientCacheEntry", new MetadataTransientCacheEntry("Key", "Value", metadata, 1));
      unmarshallAndAssertEquality("MetadataTransientMortalCacheEntry", new MetadataTransientMortalCacheEntry("Key", "Value", metadata, 1));
      unmarshallAndAssertEquality("MetadataImmortalCacheValue", new MetadataImmortalCacheValue("Value", metadata));
      unmarshallAndAssertEquality("MetadataMortalCacheValue", new MetadataMortalCacheValue("Value", metadata, 1));
      unmarshallAndAssertEquality("MetadataTransientCacheValue", new MetadataTransientCacheValue("Value", metadata, 1));
      unmarshallAndAssertEquality("MetadataTransientMortalCacheValue", new MetadataTransientMortalCacheValue("Value", metadata, 1, 1));

      byte[] bytes = "Test".getBytes();
      unmarshallAndAssertEquality("ByteBufferImpl", new ByteBufferImpl(bytes, 0, bytes.length));
      unmarshallAndAssertEquality("KeyValuePair", new KeyValuePair<>("Key", "Value"));
      InternalMetadataImpl internalMetadata = new InternalMetadataImpl(metadata, 1, 1);
      unmarshallAndAssertEquality("InternalMetadataImpl", internalMetadata);

      unmarshallAndAssertEquality("CustomExternalizer", new TestObject(1, "Test"));
   }

   private void unmarshallAndAssertEquality(String key, Object expectedObj) throws Exception {
      byte[] bytes = byteMap.get(key);
      assert bytes != null;
      Object readObj = marshaller.objectFromByteBuffer(bytes);
      assertEquals(readObj, expectedObj);
   }

   private static class TestObject {
      int id;
      String someString;

      TestObject() {
      }

      TestObject(int id, String someString) {
         this.id = id;
         this.someString = someString;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TestObject that = (TestObject) o;

         if (id != that.id) return false;
         return someString != null ? someString.equals(that.someString) : that.someString == null;

      }

      @Override
      public int hashCode() {
         int result = id;
         result = 31 * result + (someString != null ? someString.hashCode() : 0);
         return result;
      }
   }

   public static class TestObjectExternalizer implements AdvancedExternalizer<TestObject> {
      @Override
      public Set<Class<? extends TestObject>> getTypeClasses() {
         return Collections.singleton(TestObject.class);
      }

      @Override
      public Integer getId() {
         return 256;
      }

      @Override
      public void writeObject(ObjectOutput objectOutput, TestObject testObject) throws IOException {
         objectOutput.writeInt(testObject.id);
         MarshallUtil.marshallString(testObject.someString, objectOutput);
      }

      @Override
      public TestObject readObject(ObjectInput objectInput) throws IOException, ClassNotFoundException {
         TestObject testObject = new TestObject();
         testObject.id = objectInput.readInt();
         testObject.someString = MarshallUtil.unmarshallString(objectInput);
         return testObject;
      }
   }

   /**
    * Below is the program to generate the marshalled_bytes_8.x.bin file. It requires infinispan-8.2.6.Final on the classpath
    * and utilises the {@link TestObject} and {@link TestObjectExternalizer} classes defined above.
    */

   /*
public class ByteOutputGenerator {
   private static Path OUTPUT_PATH = Paths.get("target/marshalled_bytes_8.x.bin");

   public static void main(String[] args) throws Exception {
      GlobalConfiguration configuration = new GlobalConfigurationBuilder()
            .serialization().addAdvancedExternalizer(new TestObjectExternalizer()).build();
      EmbeddedCacheManager manager = new DefaultCacheManager(configuration);
      ComponentRegistry registry = manager.getCache().getAdvancedCache().getComponentRegistry();
      StreamingMarshaller marshaller = registry.getCacheMarshaller();
      OutputMap outputMap = new OutputMap(marshaller);
      generateOutput(outputMap);
      Files.write(OUTPUT_PATH, outputMap.getBytes());
   }


   private static void generateOutput(OutputMap map) throws Exception {
      map.put("List", Arrays.asList(new Person("Alan Shearer"), new Person("Nolberto Solano")));
      map.put("SingletonList", Collections.singletonList(new Key("Key", false)));
      map.put("SingletonMap", Collections.singletonMap("Key", "Value"));
      map.put("SingletonSet", Collections.singleton(new Key("Key", false)));
      map.put("KeyValuePair", new KeyValuePair<>("Key", "Value"));
      map.put("ImmortalCacheEntry", new ImmortalCacheEntry("Key", "Value"));
      map.put("MortalCacheEntry", new MortalCacheEntry("Key", "Value", 1, 1));
      map.put("TransientCacheEntry", new TransientCacheEntry("Key", "Value", 1, 1));
      map.put("TransientMortalCacheEntry", new TransientMortalCacheEntry("Key", "Value", 1, 1, 1));
      map.put("ImmortalCacheValue", new ImmortalCacheValue("Value"));
      map.put("MortalCacheValue", new MortalCacheValue("Value", 1, 1));
      map.put("TransientCacheValue", new TransientCacheValue("Value", 1, 1));
      map.put("TransientMortalCacheValue", new TransientMortalCacheValue("Value", 1, 1, 1));

      Metadata metadata = new EmbeddedMetadata.Builder().version(new NumericVersion(1)).build();
      map.put("EmbeddedMetadata", metadata);

      map.put("SimpleClusteredVersion", new SimpleClusteredVersion(1, 1));
      map.put("MetadataImmortalCacheEntry", new MetadataImmortalCacheEntry("Key", "Value", metadata));
      map.put("MetadataMortalCacheEntry", new MetadataMortalCacheEntry("Key", "Value", metadata, 1));
      map.put("MetadataTransientCacheEntry", new MetadataTransientCacheEntry("Key", "Value", metadata, 1));
      map.put("MetadataTransientMortalCacheEntry", new MetadataTransientMortalCacheEntry("Key", "Value", metadata, 1));
      map.put("MetadataImmortalCacheValue", new MetadataImmortalCacheValue("Value", metadata));
      map.put("MetadataMortalCacheValue", new MetadataMortalCacheValue("Value", metadata, 1));
      map.put("MetadataTransientCacheValue", new MetadataTransientCacheValue("Value", metadata, 1));
      map.put("MetadataTransientMortalCacheValue", new MetadataTransientMortalCacheValue("Value", metadata, 1, 1));

      byte[] bytes = "Test".getBytes();
      map.put("ByteBufferImpl", new ByteBufferImpl(bytes, 0, bytes.length));
      map.put("KeyValuePair", new KeyValuePair<>("Key", "Value"));
      InternalMetadataImpl internalMetadata = new InternalMetadataImpl(metadata, 1, 1);
      map.put("InternalMetadataImpl", internalMetadata);

      map.put("CustomExternalizer", new TestObject(1, "Test"));
   }

   private static class OutputMap {
      final Map<String, byte[]> outputMap = new HashMap<>();
      final StreamingMarshaller marshaller;

      OutputMap(StreamingMarshaller marshaller) {
         this.marshaller = marshaller;
      }

      void put(String key, Object object) throws Exception {
         outputMap.put(key, marshaller.objectToByteBuffer(object));
      }

      byte[] getBytes() throws Exception {
         return marshaller.objectToByteBuffer(outputMap);
      }
   }
}
   */
}
