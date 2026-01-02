package org.infinispan.client.hotrod.marshall.protostream.validation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.schema.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.marshall.protostream.validation.ProtobufValidationTest")
@TestForIssue(jiraKey = "ISPN-14816")
public class ProtobufValidationTest extends SingleHotRodServerTest {

   private static final String SCHEMA_NAME = "my-schema.proto";
   public static final String ERROR_WHILE_PARSING_MY_SCHEMA_PROTO = "IPROTO000013: Error while parsing 'my-schema.proto'";

   private RemoteSchemasAdmin schemasAdmin;
   private String goodSchema;
   private String wrongSchema;

   @BeforeClass
   public void beforeAll() throws Exception {
      schemasAdmin = remoteCacheManager.administration().schemas();
      goodSchema = Util.getResourceAsString("/proto/ciao.proto", getClass().getClassLoader());
      wrongSchema = Util.getResourceAsString("/proto/ciao-wrong.proto", getClass().getClassLoader());
   }

   @AfterMethod
   public void afterEach() {
      schemasAdmin.remove(SCHEMA_NAME, true);
   }

   @Test
   public void testGoodSchema() {
      schemasAdmin.createOrUpdate(Schema.buildFromStringContent(SCHEMA_NAME, goodSchema));
      assertThat(schemasAdmin.retrieveError(SCHEMA_NAME)).isEmpty();
   }

   @Test
   public void testWrongSchema() {
      schemasAdmin.createOrUpdate(Schema.buildFromStringContent(SCHEMA_NAME, wrongSchema));
      Optional<String> errorOpt = schemasAdmin.retrieveError(SCHEMA_NAME);
      assertThat(errorOpt).isNotEmpty();
      assertThat(errorOpt.get()).contains(ERROR_WHILE_PARSING_MY_SCHEMA_PROTO);
   }

   @Test
   public void testUpdateWithWrongSchema() {
      schemasAdmin.createOrUpdate(Schema.buildFromStringContent(SCHEMA_NAME, goodSchema));
      Optional<String> errorOpt = schemasAdmin.retrieveError(SCHEMA_NAME);
      assertThat(errorOpt).isEmpty();
      assertThat(schemasAdmin.get(SCHEMA_NAME)).isNotEmpty();

      schemasAdmin.createOrUpdate(Schema.buildFromStringContent(SCHEMA_NAME, wrongSchema));

      errorOpt = schemasAdmin.retrieveError(SCHEMA_NAME);
      assertThat(errorOpt).isNotEmpty();
      assertThat(errorOpt.get()).contains(ERROR_WHILE_PARSING_MY_SCHEMA_PROTO);
   }
}
