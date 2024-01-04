package org.infinispan.protostream.sampledomain;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = {
            Address.class,
            Account.class,
            Account.Currency.class,
            Account.Limits.class,
            KeywordVector.class,
            Note.class,
            Transaction.class,
            User.class,
            User.Gender.class
      },
      schemaFileName = "test.protostream.sampledomain.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "sample_bank_account",
      service = false
)
public interface TestDomainSCI extends GeneratedSchema {
   GeneratedSchema INSTANCE = new TestDomainSCIImpl();
}
