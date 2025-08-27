package org.infinispan.query.remote.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.infinispan.server.core.query.impl.ProtobufMetadataManagerImpl;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@SuppressWarnings("resource")
@Test(groups = "functional", testName = "query.remote.impl.ZeroCapacityNodeProtobufMetadataTest")
public class ZeroCapacityNodeProtobufMetadataTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      var builder = GlobalConfigurationBuilder.defaultClusteredBuilder().zeroCapacityNode(true);
      addClusterEnabledCacheManager(builder, null);
      waitForClusterToForm(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
   }

   public void testSchemaRegister() {
      assertTrue(manager(1).getCacheManagerConfiguration().isZeroCapacityNode());
      protobufCache(0).put(dummyProtoFileName("a"), dummyProtoFile("a"));
      protobufCache(1).put(dummyProtoFileName("b"), dummyProtoFile("b"));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "a");
         checkProtoFileExists(m, "b");
      }
   }

   public void testNodeJoins() {
      assertTrue(manager(1).getCacheManagerConfiguration().isZeroCapacityNode());
      protobufCache(0).put(dummyProtoFileName("c"), dummyProtoFile("c"));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "c");
      }

      addClusterEnabledCacheManager();
      waitForClusterToForm(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "c");
      }
   }

   public void testZeroCapacityNodeJoins() {
      assertTrue(manager(1).getCacheManagerConfiguration().isZeroCapacityNode());
      protobufCache(0).put(dummyProtoFileName("d"), dummyProtoFile("d"));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "d");
      }

      var builder = GlobalConfigurationBuilder.defaultClusteredBuilder().zeroCapacityNode(true);
      addClusterEnabledCacheManager(builder, null);
      waitForClusterToForm(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "d");
      }
   }

   public void testMultipleSchemaRegister() {
      assertTrue(manager(1).getCacheManagerConfiguration().isZeroCapacityNode());
      protobufCache(0).putAll(Map.of(
            dummyProtoFileName("e"), dummyProtoFile("e"),
            dummyProtoFileName("f"), dummyProtoFile("f"),
            dummyProtoFileName("g"), dummyProtoFile("g")
      ));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "e");
         checkProtoFileExists(m, "f");
         checkProtoFileExists(m, "g");
      }
   }

   public void testReplace() {
      assertTrue(manager(1).getCacheManagerConfiguration().isZeroCapacityNode());
      protobufCache(0).put(dummyProtoFileName("h"), dummyProtoFile("h"));
      protobufCache(1).put(dummyProtoFileName("i"), dummyProtoFile("i"));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "h");
         checkProtoFileExists(m, "i");
      }

      protobufCache(0).replace(dummyProtoFileName("i"), dummyProtoFile("ii"));
      protobufCache(1).replace(dummyProtoFileName("h"), dummyProtoFile("hh"));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "h", "hh");
         checkProtoFileExists(m, "i", "ii");
      }
   }

   public void testClear() {
      assertTrue(manager(1).getCacheManagerConfiguration().isZeroCapacityNode());
      protobufCache(0).put(dummyProtoFileName("j"), dummyProtoFile("j"));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "j");
      }

      protobufCache(1).clear();

      for (var m : cacheManagers) {
         checkProtoFileDoesNotExists(m, "j");
      }

      protobufCache(1).put(dummyProtoFileName("k"), dummyProtoFile("k"));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "k");
      }

      protobufCache(0).clear();

      for (var m : cacheManagers) {
         checkProtoFileDoesNotExists(m, "k");
      }
   }

   public void testRemove() {
      assertTrue(manager(1).getCacheManagerConfiguration().isZeroCapacityNode());
      protobufCache(0).put(dummyProtoFileName("l"), dummyProtoFile("l"));
      protobufCache(1).put(dummyProtoFileName("m"), dummyProtoFile("m"));

      for (var m : cacheManagers) {
         checkProtoFileExists(m, "l");
         checkProtoFileExists(m, "m");
      }

      protobufCache(0).remove(dummyProtoFileName("m"));
      protobufCache(1).remove(dummyProtoFileName("l"));

      for (var m : cacheManagers) {
         checkProtoFileDoesNotExists(m, "m");
         checkProtoFileDoesNotExists(m, "l");
      }
   }

   private Cache<String, String> protobufCache(int index) {
      return cache(index, ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
   }

   private static SerializationContextRegistry serializationContextRegistry(EmbeddedCacheManager manager) {
      return TestingUtil.extractGlobalComponent(manager, SerializationContextRegistry.class);
   }

   private static void checkProtoFileExists(EmbeddedCacheManager cacheManager, String packageName) {
      checkProtoFileExists(cacheManager, packageName, packageName);
   }

   private static void checkProtoFileExists(EmbeddedCacheManager cacheManager, String fileName, String packageName) {
      var name = dummyProtoFileName(fileName);
      assertTrue(serializationContext(cacheManager).getFileDescriptors().containsKey(name));
      assertEquals(packageName, serializationContext(cacheManager).getFileDescriptors().get(name).getPackage());

      assertTrue(serializationContextRegistry(cacheManager).getUserCtx().getFileDescriptors().containsKey(name));
      assertEquals(packageName, serializationContextRegistry(cacheManager).getUserCtx().getFileDescriptors().get(name).getPackage());
   }

   private static void checkProtoFileDoesNotExists(EmbeddedCacheManager cacheManager, String fileName) {
      var name = dummyProtoFileName(fileName);
      assertFalse(serializationContext(cacheManager).getFileDescriptors().containsKey(name));
      assertFalse(serializationContextRegistry(cacheManager).getUserCtx().getFileDescriptors().containsKey(name));
   }

   private static SerializationContext serializationContext(EmbeddedCacheManager manager) {
      var protobufMetadataManager = TestingUtil.extractGlobalComponent(manager, ProtobufMetadataManager.class);
      assert protobufMetadataManager instanceof ProtobufMetadataManagerImpl;
      return ((ProtobufMetadataManagerImpl) protobufMetadataManager).getSerializationContext();
   }

   private static String dummyProtoFile(String packageName) {
      return "package " + packageName + ";";
   }

   private static String dummyProtoFileName(String packageName) {
      return packageName + ".proto";
   }
}
