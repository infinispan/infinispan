package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.CalculusManual;
import org.infinispan.client.hotrod.query.testdomain.protobuf.LimitsPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.TransactionPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.types.java.CommonTypes;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.User;

@AutoProtoSchemaBuilder(
      dependsOn = CommonTypes.class,
      includeClasses = {
            AddressPB.class,
            AccountPB.class,
            Account.Currency.class,
            CalculusManual.class,
            LimitsPB.class,
            TransactionPB.class,
            UserPB.class,
            User.Gender.class
      },
      schemaFileName = "test.client.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "sample_bank_account",
      service = false
)
public interface TestDomainSCI extends GeneratedSchema {
   GeneratedSchema INSTANCE = new TestDomainSCIImpl();
}
