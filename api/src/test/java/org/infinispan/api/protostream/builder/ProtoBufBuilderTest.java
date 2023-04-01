package org.infinispan.api.protostream.builder;

public class ProtoBufBuilderTest {

   public void testProtoBuilder() {
      ProtoBuf.builder()
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
   }
}
