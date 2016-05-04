package org.infinispan.query.remote.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test that ProtobufIndexedFieldProvider correctly identifies indexed fields based on the protostream annotations
 * applied in the test model (bank.proto).
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.remote.impl.ProtobufIndexedFieldProviderTest")
public class ProtobufIndexedFieldProviderTest extends SingleCacheManagerTest {

   private static final String PROTO_DEFINITIONS =
         "/** @Indexed */ message User {\n" +
               "\n" +
               "   /** @IndexedField */ required string name = 1;\n" +
               "\n" +
               "   required string surname = 2;\n" +
               "\n" +
               "   /** @Indexed(true) */" +
               "   message Address {\n" +
               "      /** @IndexedField */ required string street = 10;\n" +
               "      required string postCode = 20;\n" +
               "   }\n" +
               "\n" +
               "   /** @IndexedField */ repeated Address indexedAddresses = 3; \n" +
               "\n" +
               "   /** @IndexedField(index=false, store=false) */ repeated Address unindexedAddresses = 4;\n" +
               "}\n";

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testProvider() throws Exception {
      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("user_definition.proto", PROTO_DEFINITIONS));
      ProtobufIndexedFieldProvider userIndexedFieldProvider = new ProtobufIndexedFieldProvider(serCtx.getMessageDescriptor("User"));
      ProtobufIndexedFieldProvider addressIndexedFieldProvider = new ProtobufIndexedFieldProvider(serCtx.getMessageDescriptor("User.Address"));

      // try top level attributes
      assertTrue(userIndexedFieldProvider.isIndexed(new String[]{"name"}));
      assertFalse(userIndexedFieldProvider.isIndexed(new String[]{"surname"}));
      assertTrue(addressIndexedFieldProvider.isIndexed(new String[]{"street"}));
      assertFalse(addressIndexedFieldProvider.isIndexed(new String[]{"postCode"}));

      // try nested attributes
      assertTrue(userIndexedFieldProvider.isIndexed(new String[]{"indexedAddresses", "street"}));
      assertFalse(userIndexedFieldProvider.isIndexed(new String[]{"indexedAddresses", "postCode"}));
      assertFalse(userIndexedFieldProvider.isIndexed(new String[]{"unindexedAddresses", "street"}));
      assertFalse(userIndexedFieldProvider.isIndexed(new String[]{"unindexedAddresses", "postCode"}));
   }
}
