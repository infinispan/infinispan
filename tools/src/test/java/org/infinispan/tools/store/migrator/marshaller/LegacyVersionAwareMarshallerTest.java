package org.infinispan.tools.store.migrator.marshaller;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.tools.store.migrator.TestUtil;
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
      externalizerMap.put(256, new TestUtil.TestObjectExternalizer());
      marshaller = new LegacyVersionAwareMarshaller(externalizerMap);
   }

   @BeforeClass(alwaysRun = true)
   public void beforeTest() throws Exception {
      Path path = new File("src/test/resources/marshalled_bytes_8.x.bin").toPath();
      byte[] bytes = Files.readAllBytes(path);
      byteMap = (Map<String, byte[]>) marshaller.objectFromByteBuffer(bytes);
   }

   public void testUnmarshalling() throws Exception {
      for (Map.Entry<String, Object> entry : TestUtil.TEST_MAP.entrySet())
         unmarshallAndAssertEquality(entry.getKey(), entry.getValue());
   }

   private void unmarshallAndAssertEquality(String key, Object expectedObj) throws Exception {
      byte[] bytes = byteMap.get(key);
      assert bytes != null;
      Object readObj = marshaller.objectFromByteBuffer(bytes);
      assertEquals(readObj, expectedObj);
   }

   /**
    * Below is the program to generate the marshalled_bytes_8.x.bin file. It requires infinispan-8.2.6.Final on the classpath
    * and utilises the {@link TestObject} and {@link TestObjectExternalizer} classes defined above.
    */

   /*
public class ByteOutputGenerator {

   public static void main(String[] args) throws Exception {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .serialization().addAdvancedExternalizer(new TestObjectExternalizer()).build();
      Configuration config = new ConfigurationBuilder().persistence()
            .addStore(LevelDBStoreConfigurationBuilder.class)
            .build();
      EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig, config);
      ComponentRegistry registry = manager.getCache().getAdvancedCache().getComponentRegistry();
      StreamingMarshaller marshaller = registry.getCacheMarshaller();

      // Write to stores
      generateOutput(new CacheStoreOutput(manager.getCache("RocksDBReaderTest")));

      // Binary file
      ByteOutputMap outputMap = new ByteOutputMap(marshaller);
      generateOutput(outputMap);
      Files.write(Paths.get("target/marshalled_bytes_8.x.bin"), outputMap.getBytes());
   }


   private static void generateOutput(Output output) throws Exception {
      output.put("List", Arrays.asList(new Person("Alan Shearer"), new Person("Nolberto Solano")));
      output.put("SingletonList", Collections.singletonList(new Key("Key", false)));
      output.put("SingletonMap", Collections.singletonMap("Key", "Value"));
      output.put("SingletonSet", Collections.singleton(new Key("Key", false)));
      output.put("KeyValuePair", new KeyValuePair<>("Key", "Value"));
      output.put("ImmortalCacheEntry", new ImmortalCacheEntry("Key", "Value"));
      output.put("MortalCacheEntry", new MortalCacheEntry("Key", "Value", 1, 1));
      output.put("TransientCacheEntry", new TransientCacheEntry("Key", "Value", 1, 1));
      output.put("TransientMortalCacheEntry", new TransientMortalCacheEntry("Key", "Value", 1, 1, 1));
      output.put("ImmortalCacheValue", new ImmortalCacheValue("Value"));
      output.put("MortalCacheValue", new MortalCacheValue("Value", 1, 1));
      output.put("TransientCacheValue", new TransientCacheValue("Value", 1, 1));
      output.put("TransientMortalCacheValue", new TransientMortalCacheValue("Value", 1, 1, 1));

      Metadata metadata = new EmbeddedMetadata.Builder().version(new NumericVersion(1)).build();
      output.put("EmbeddedMetadata", metadata);

      output.put("SimpleClusteredVersion", new SimpleClusteredVersion(1, 1));
      output.put("MetadataImmortalCacheEntry", new MetadataImmortalCacheEntry("Key", "Value", metadata));
      output.put("MetadataMortalCacheEntry", new MetadataMortalCacheEntry("Key", "Value", metadata, 1));
      output.put("MetadataTransientCacheEntry", new MetadataTransientCacheEntry("Key", "Value", metadata, 1));
      output.put("MetadataTransientMortalCacheEntry", new MetadataTransientMortalCacheEntry("Key", "Value", metadata, 1));
      output.put("MetadataImmortalCacheValue", new MetadataImmortalCacheValue("Value", metadata));
      output.put("MetadataMortalCacheValue", new MetadataMortalCacheValue("Value", metadata, 1));
      output.put("MetadataTransientCacheValue", new MetadataTransientCacheValue("Value", metadata, 1));
      output.put("MetadataTransientMortalCacheValue", new MetadataTransientMortalCacheValue("Value", metadata, 1, 1));

      byte[] bytes = "Test".getBytes();
      output.put("ByteBufferImpl", new ByteBufferImpl(bytes, 0, bytes.length));
      output.put("KeyValuePair", new KeyValuePair<>("Key", "Value"));
      InternalMetadataImpl internalMetadata = new InternalMetadataImpl(metadata, 1, 1);
      output.put("InternalMetadataImpl", internalMetadata);

      output.put("CustomExternalizer", new TestObject(1, "Test"));
   }

   interface Output {
      void put(String key, Object object) throws Exception;
   }

   private static class CacheStoreOutput implements Output {

      final Cache cache;

      public CacheStoreOutput(Cache cache) {
         this.cache = cache;
      }

      @Override
      public void put(String key, Object object) throws Exception {
         cache.put(key, object);
      }
   }

   private static class ByteOutputMap implements Output {
      final Map<String, byte[]> outputMap = new HashMap<>();
      final StreamingMarshaller marshaller;

      ByteOutputMap(StreamingMarshaller marshaller) {
         this.marshaller = marshaller;
      }

      @Override
      public void put(String key, Object object) throws Exception {
         outputMap.put(key, marshaller.objectToByteBuffer(object));
      }

      byte[] getBytes() throws Exception {
         return marshaller.objectToByteBuffer(outputMap);
      }
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
      public TestObject readObject(ObjectInput objectInput) throws IOException {
         TestObject testObject = new TestObject();
         testObject.id = objectInput.readInt();
         testObject.someString = MarshallUtil.unmarshallString(objectInput);
         return testObject;
      }
   }
}
   */
}
