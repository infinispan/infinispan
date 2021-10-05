package org.infinispan.query.remote.impl;

import static java.util.Collections.singletonList;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.setOf;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.impl.AnnotatedDescriptorImpl;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.remote.impl.ProtobufMetadataCachePreserveStateAcrossRestartsTest")
public class ProtobufMetadataCachePreserveStateAcrossRestartsTest extends AbstractInfinispanTest {

   protected EmbeddedCacheManager createCacheManager(String persistentStateLocation) throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().clusteredDefault();
      global.globalState().enable().persistentLocation(persistentStateLocation);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(global, new ConfigurationBuilder());
      cacheManager.getCache();
      return cacheManager;
   }

   public void testStatePreserved1Node() throws Exception {
      String persistentStateLocation = CommonsTestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(persistentStateLocation);

      TestingUtil.withCacheManager(new CacheManagerCallable(createCacheManager(persistentStateLocation)) {
         @Override
         public void call() {
            insertSchemas(this.cm);

            verifySchemas(cm);
         }

      });

      TestingUtil.withCacheManager(new CacheManagerCallable(createCacheManager(persistentStateLocation)) {
         @Override
         public void call() {
            verifySchemas(cm);
         }
      });
   }

   public void testStatePreserved2Nodes() throws Exception {
      String persistentStateLocation = CommonsTestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(persistentStateLocation);

      final String persistentStateLocation1 = persistentStateLocation + "/1";
      final String persistentStateLocation2 = persistentStateLocation + "/2";
      TestingUtil.withCacheManagers(new MultiCacheManagerCallable(createCacheManager(persistentStateLocation1),
                                                                  createCacheManager(persistentStateLocation2)) {
         @Override
         public void call() {
            insertSchemas(cms[0]);

            verifySchemas(cms[0]);
            verifySchemas(cms[1]);
         }
      });

      TestingUtil.withCacheManagers(new MultiCacheManagerCallable(createCacheManager(persistentStateLocation1),
                                                                  createCacheManager(persistentStateLocation2)) {
         @Override
         public void call() {
            verifySchemas(cms[0]);
            verifySchemas(cms[0]);
         }
      });
   }

   private void insertSchemas(EmbeddedCacheManager cm) {
      Cache<String, String> protobufMetadaCache = cm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      // Valid schemas
      protobufMetadaCache.put("A.proto", "package A;");
      protobufMetadaCache.put("B.proto", "import \"A.proto\";\npackage B;");
      protobufMetadaCache.put("C.proto", "import \"A.proto\";\nimport \"B.proto\";\npackage C;");
      protobufMetadaCache.put("D.proto", "import \"B.proto\";\nimport \"C.proto\";\npackage D;\n" +
                                         "message M {\nrequired string s = 1;\n}");
      // Schemas with errors
      protobufMetadaCache.put("E.proto", "import \"E.proto\";\npackage E;");
      protobufMetadaCache.put("F.proto", "import \"E.proto\";\nimport \"X.proto\";\npackage E;");
   }

   private void verifySchemas(CacheContainer manager) {
      ProtobufMetadataManager pmm = extractGlobalComponent(manager, ProtobufMetadataManager.class);
      assertEquals(setOf("E.proto", "F.proto"), setOf(pmm.getFilesWithErrors()));
      Set<String> protoNames = setOf("A.proto", "B.proto", "C.proto", "D.proto", "E.proto", "F.proto");
      assertEquals(protoNames, setOf(pmm.getProtofileNames()));

      SerializationContextRegistry scr = extractGlobalComponent(manager, SerializationContextRegistry.class);
      ImmutableSerializationContext serializationContext = scr.getGlobalCtx();
      Descriptor mDescriptor = serializationContext.getMessageDescriptor("D.M");
      assertNotNull(mDescriptor);
      List<String> fields = mDescriptor.getFields().stream()
                                       .map(AnnotatedDescriptorImpl::getName)
                                       .collect(Collectors.toList());
      assertEquals(singletonList("s"), fields);
   }
}
