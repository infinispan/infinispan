package org.infinispan.query.remote.impl;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Test that ProtobufIndexedFieldProvider correctly identifies indexed fields based on the protostream annotations
 * applied in the test model (bank.proto).
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.remote.impl.ProtobufFieldIndexingMetadataTest")
public class ProtobufFieldIndexingMetadataTest extends SingleCacheManagerTest {

   private static final class TestSCI implements SerializationContextInitializer {
      private static final String PROTO_FILE = """
            /** @Indexed */ message User {
               /** @Basic(projectable = true) */ required string name = 1;
               required string surname = 2;
               /** @Indexed */
               message Address {
                  /** @Basic(projectable = true) */ required string street = 10;
                  required string postCode = 20;
               }
               /** @Embedded */ repeated Address indexedAddresses = 3;
               repeated Address unindexedAddresses = 4;
            }
            """;

      @Override
      public void registerSchema(SerializationContext serCtx) {
         serCtx.registerProtoFiles(FileDescriptorSource.fromString("user_definition.proto", PROTO_FILE));
      }

      @Override
      public void registerMarshallers(SerializationContext ctx) {
      }
   }

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("User");
      return TestCacheManagerFactory.createServerModeCacheManager(new TestSCI(), cfg);
   }

   public void testProtobufFieldIndexingMetadata() {
      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
      ProtobufFieldIndexingMetadata userIndexedFieldProvider =
            new ProtobufFieldIndexingMetadata(serCtx.getMessageDescriptor("User"), serCtx.getGenericDescriptors());
      ProtobufFieldIndexingMetadata addressIndexedFieldProvider =
            new ProtobufFieldIndexingMetadata(serCtx.getMessageDescriptor("User.Address"), serCtx.getGenericDescriptors());

      // try top level attributes
      assertTrue(userIndexedFieldProvider.isSearchable(new String[]{"name"}));
      assertFalse(userIndexedFieldProvider.isSearchable(new String[]{"surname"}));
      assertTrue(addressIndexedFieldProvider.isSearchable(new String[]{"street"}));
      assertFalse(addressIndexedFieldProvider.isSearchable(new String[]{"postCode"}));

      // try nested attributes
      assertTrue(userIndexedFieldProvider.isSearchable(new String[]{"indexedAddresses", "street"}));
      assertFalse(userIndexedFieldProvider.isSearchable(new String[]{"indexedAddresses", "postCode"}));
      assertFalse(userIndexedFieldProvider.isSearchable(new String[]{"unindexedAddresses", "street"}));
      assertFalse(userIndexedFieldProvider.isSearchable(new String[]{"unindexedAddresses", "postCode"}));
   }
}
