package org.infinispan.protostream.sampledomain;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.infinispan.protostream.sampledomain.bank.Address;
import org.infinispan.protostream.sampledomain.bank.Transaction;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.protostream.types.java.CommonTypes;

@ProtoSchema(
      dependsOn = CommonTypes.class,
      includeClasses = {
            Address.class,
            Account.class,
            Account.Currency.class,
            Account.Limits.class,
            CalculusManual.class,
            FlightRoute.class,
            KeywordVector.class,
            NotIndexed.class,
            Transaction.class,
            User.class,
            User.Gender.class,
            Metadata.class
      },
      schemaFileName = "test.protostream.sampledomain.proto",
      schemaFilePath = "org/infinispan/test",
      schemaPackageName = "sample_domain",
      service = false
)
public interface TestDomainSCI extends GeneratedSchema {
   GeneratedSchema INSTANCE = new TestDomainSCIImpl();
}
