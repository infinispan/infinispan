package org.infinispan.query.dsl.embedded;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.query.dsl.embedded.testdomain.Currency;
import org.infinispan.query.dsl.embedded.testdomain.Gender;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AddressHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.LimitsHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.query.test.QueryTestSCI;

@AutoProtoSchemaBuilder(
      dependsOn = QueryTestSCI.class,
      includeClasses = {
            AccountHS.class,
            AddressHS.class,
            Currency.class,
            Gender.class,
            LimitsHS.class,
            NotIndexed.class,
            TransactionHS.class,
            UserHS.class
      },
      schemaFileName = "test.query.dsl.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.query.dsl")
public interface DslSCI extends SerializationContextInitializer {
   DslSCI INSTANCE = new DslSCIImpl();
}
