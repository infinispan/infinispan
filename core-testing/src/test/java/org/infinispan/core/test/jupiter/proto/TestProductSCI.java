package org.infinispan.core.test.jupiter.proto;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      includeClasses = Product.class,
      schemaFileName = "test.product.proto",
      schemaFilePath = "/",
      schemaPackageName = "org.infinispan.test.jupiter.proto",
      service = false
)
public interface TestProductSCI extends SerializationContextInitializer {
   TestProductSCI INSTANCE = new TestProductSCIImpl();
}
