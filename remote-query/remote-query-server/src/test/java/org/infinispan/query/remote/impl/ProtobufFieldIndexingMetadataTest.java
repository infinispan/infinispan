package org.infinispan.query.remote.impl;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
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
               "   /** @IndexedField */ repeated Address indexedAddresses = 3;\n" +
               "\n" +
               "   /** @IndexedField(index=false, store=false) */ repeated Address unindexedAddresses = 4;\n" +
               "}";

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testProtobufFieldIndexingMetadata() throws Exception {
      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("user_definition.proto", PROTO_DEFINITIONS));
      ProtobufFieldIndexingMetadata userIndexedFieldProvider = new ProtobufFieldIndexingMetadata(serCtx.getMessageDescriptor("User"));
      ProtobufFieldIndexingMetadata addressIndexedFieldProvider = new ProtobufFieldIndexingMetadata(serCtx.getMessageDescriptor("User.Address"));

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

   @Test(expectedExceptions = DescriptorParserException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: Annotation 'IndexedField' cannot be used together with 'Field' on field test.User.age")
   public void testIndexedFieldAndField() throws Exception {
      String testProto = "package test;\n" +
            "/* @Indexed */ message User {\n" +
            "   /* @IndexedField(index=false, store=false) @Field(store=Store.NO, index=Index.NO) */ " +
            "   required int32 age = 1;\n" +
            "}";

      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("test.proto", testProto));
   }

   @Test(expectedExceptions = DescriptorParserException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: Annotation 'IndexedField' cannot be used together with 'SortableField' on field test.User1.age")
   public void testIndexedFieldAndSortableField() throws Exception {
      String testProto = "package test;\n" +
            "/* @Indexed */ message User1 {\n" +
            "   /* @IndexedField(index=false, store=false) @SortableField */ " +
            "   required int32 age = 1;\n" +
            "}";

      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("test1.proto", testProto));
   }

   @Test(expectedExceptions = DescriptorParserException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: Annotation 'IndexedField' cannot be used together with 'Analyzer' on field test.User2.name")
   public void testIndexedFieldAndAnalyzer() throws Exception {
      String testProto = "package test;\n" +
            "/* @Indexed */" +
            "message User2 {\n" +
            "   /* @IndexedField(index=false, store=false) @Analyzer(definition=\"standard\") */" +
            "   required string name = 1;\n" +
            "}";

      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("test1.proto", testProto));
   }

   @Test(expectedExceptions = DescriptorParserException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: Cannot specify an analyzer for field test.User3.name unless the field is analyzed.")
   public void testAnalyzerForNotAnalyzedField() throws Exception {
      String testProto = "package test;\n" +
            "/* @Indexed */ message User3 {\n" +
            "   /* @Field(store=Store.NO, index=Index.YES, analyze=Analyze.NO, analyzer=@Analyzer(definition=\"standard\")) */" +
            "   required string name = 1;\n" +
            "}";

      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("test2.proto", testProto));
   }
}
