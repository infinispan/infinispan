package org.infinispan.client.hotrod.marshall.protostream.validation;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.protostream.impl.ResourceUtils;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.marshall.protostream.validation.ProtobufValidationTest")
@TestForIssue(jiraKey = "ISPN-14816")
public class ProtobufValidationTest extends SingleHotRodServerTest {

   private static final String SCHEMA_NAME = "my-schema.proto";
   public static final String SCHEMA_ERROR_KEY = SCHEMA_NAME + ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX;

   private Cache<String, String> metadataCache;
   private String goodSchema;
   private String wrongSchema;

   @BeforeClass
   public void beforeAll() {
      metadataCache = cacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      goodSchema = ResourceUtils.getResourceAsString(getClass(), "/proto/ciao.proto");
      wrongSchema = ResourceUtils.getResourceAsString(getClass(), "/proto/ciao-wrong.proto");
   }

   @AfterMethod
   public void afterEach() {
      metadataCache.clear();
   }

   @Test
   public void testGoodSchema() {
      metadataCache.put(SCHEMA_NAME, goodSchema);

      String filesWithErrors = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
      assertThat(filesWithErrors).isNull();
   }

   @Test
   public void testWrongSchema() {
      metadataCache.put(SCHEMA_NAME, wrongSchema);

      String filesWithErrors = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
      assertThat(filesWithErrors).isEqualTo(SCHEMA_NAME);
      String errorMessage = metadataCache.get(SCHEMA_ERROR_KEY);
      assertThat(errorMessage).isNotBlank();
   }

   @Test
   public void testUpdateWithWrongSchema() {
      metadataCache.put(SCHEMA_NAME, goodSchema);

      String filesWithErrors = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
      assertThat(filesWithErrors).isNull();

      metadataCache.put(SCHEMA_NAME, wrongSchema);

      filesWithErrors = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
      assertThat(filesWithErrors).isEqualTo(SCHEMA_NAME);
      String errorMessage = metadataCache.get(SCHEMA_ERROR_KEY);
      assertThat(errorMessage).isNotBlank();
   }
}
