package org.infinispan.tools.store.migrator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
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

public class TestUtil {
   public static final Map<String, Object> TEST_MAP = new HashMap<>();
   static {
      TEST_MAP.put("List", Arrays.asList(new Person("Alan Shearer"), new Person("Nolberto Solano")));
      TEST_MAP.put("SingletonList", Collections.singletonList(new Key("Key", false)));
      TEST_MAP.put("SingletonMap", Collections.singletonMap("Key", "Value"));
      TEST_MAP.put("SingletonSet", Collections.singleton(new Key("Key", false)));
      TEST_MAP.put("KeyValuePair", new KeyValuePair<>("Key", "Value"));
      TEST_MAP.put("ImmortalCacheEntry", new ImmortalCacheEntry("Key", "Value"));
      TEST_MAP.put("MortalCacheEntry", new MortalCacheEntry("Key", "Value", 1, 1));
      TEST_MAP.put("TransientCacheEntry", new TransientCacheEntry("Key", "Value", 1, 1));
      TEST_MAP.put("TransientMortalCacheEntry", new TransientMortalCacheEntry("Key", "Value", 1, 1, 1));
      TEST_MAP.put("ImmortalCacheValue", new ImmortalCacheValue("Value"));
      TEST_MAP.put("MortalCacheValue", new MortalCacheValue("Value", 1, 1));
      TEST_MAP.put("TransientCacheValue", new TransientCacheValue("Value", 1, 1));
      TEST_MAP.put("TransientMortalCacheValue", new TransientMortalCacheValue("Value", 1, 1, 1));

      Metadata metadata = new EmbeddedMetadata.Builder().version(new NumericVersion(1)).build();
      TEST_MAP.put("EmbeddedMetadata", metadata);

      TEST_MAP.put("SimpleClusteredVersion", new SimpleClusteredVersion(1, 1));
      TEST_MAP.put("MetadataImmortalCacheEntry", new MetadataImmortalCacheEntry("Key", "Value", metadata));
      TEST_MAP.put("MetadataMortalCacheEntry", new MetadataMortalCacheEntry("Key", "Value", metadata, 1));
      TEST_MAP.put("MetadataTransientCacheEntry", new MetadataTransientCacheEntry("Key", "Value", metadata, 1));
      TEST_MAP.put("MetadataTransientMortalCacheEntry", new MetadataTransientMortalCacheEntry("Key", "Value", metadata, 1));
      TEST_MAP.put("MetadataImmortalCacheValue", new MetadataImmortalCacheValue("Value", metadata));
      TEST_MAP.put("MetadataMortalCacheValue", new MetadataMortalCacheValue("Value", metadata, 1));
      TEST_MAP.put("MetadataTransientCacheValue", new MetadataTransientCacheValue("Value", metadata, 1));
      TEST_MAP.put("MetadataTransientMortalCacheValue", new MetadataTransientMortalCacheValue("Value", metadata, 1, 1));

      byte[] bytes = "Test".getBytes();
      TEST_MAP.put("ByteBufferImpl", new ByteBufferImpl(bytes, 0, bytes.length));
      TEST_MAP.put("KeyValuePair", new KeyValuePair<>("Key", "Value"));
      InternalMetadataImpl internalMetadata = new InternalMetadataImpl(metadata, 1, 1);
      TEST_MAP.put("InternalMetadataImpl", internalMetadata);

      TEST_MAP.put("CustomExternalizer", new TestObject(1, "Test"));
   }

   public static class TestObject {
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

   public static String propKey(Element... elements) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < elements.length; i++) {
         sb.append(elements[i].toString());
         if (i != elements.length - 1) sb.append(".");
      }
      return sb.toString();
   }
}
