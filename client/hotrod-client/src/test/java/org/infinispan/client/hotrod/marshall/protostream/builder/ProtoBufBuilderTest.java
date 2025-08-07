package org.infinispan.client.hotrod.marshall.protostream.builder;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.api.protostream.builder.ProtoBuf;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.impl.ResourceUtils;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.marshall.protostream.builder.ProtoBufBuilderTest")
@TestForIssue(jiraKey = "ISPN-14724")
public class ProtoBufBuilderTest extends SingleHotRodServerTest {

   @Test
   private void testGeneratedSchema() {
      String generatedSchema = ProtoBuf.builder()
         .packageName("org.infinispan")
            .message("author") // protobuf message is usually lowercase
            .indexed()
               .required("name", 1, "string")
                  .basic()
                     .sortable(true)
                     .projectable(true)
               .optional("age", 2, "int32")
                  .keyword()
                     .sortable(true)
                     .aggregable(true)
            .message("book")
            .indexed()
               .required("title", 1, "string")
                  .basic()
                     .projectable(true)
               .optional("yearOfPublication", 2, "int32")
                  .keyword()
                     .normalizer("lowercase")
               .optional("description", 3, "string")
                  .text()
                     .analyzer("english")
                     .searchAnalyzer("whitespace")
               .required("author", 4, "author")
                  .embedded()
      .build();

      remoteCacheManager.administration().schemas().createOrUpdate(FileDescriptorSource.fromString("ciao.proto", generatedSchema));
      assertThat( remoteCacheManager.administration().schemas().retrieveAllSchemaErrors().isEmpty()).isTrue();

      String expectedSchema = ResourceUtils.getResourceAsString(getClass(), "/proto/ciao.proto");
      assertThat(generatedSchema).isEqualTo(expectedSchema);
   }

}
