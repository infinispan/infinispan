package org.infinispan.test;

import org.infinispan.distribution.MagicKey;
import org.infinispan.expiration.impl.ExpirationFunctionalTest;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.BrokenMarshallingPojo;
import org.infinispan.test.data.CountMarshallingPojo;
import org.infinispan.test.data.DelayedMarshallingPojo;
import org.infinispan.test.data.Key;
import org.infinispan.test.data.Numerics;
import org.infinispan.test.data.Person;
import org.infinispan.test.data.Sex;
import org.infinispan.test.data.Value;
import org.infinispan.xsite.irac.IracCustomConflictTest;

@ProtoSchema(
      // TODO re-evaluate use of Person where Value is more appropriate
      includeClasses = {
            Address.class,
            BrokenMarshallingPojo.class,
            DelayedMarshallingPojo.class,
            Key.class,
            MagicKey.class,
            CountMarshallingPojo.class,
            Person.class,
            Sex.class,
            Value.class,
            Numerics.class,
            IracCustomConflictTest.MySortedSet.class,
            ExpirationFunctionalTest.NoEquals.class,
      },
      schemaFileName = "test.core.proto",
      schemaFilePath = "org/infinispan",
      schemaPackageName = "org.infinispan.test.core",
      service = false,
      orderedMarshallers = true
)
public interface TestDataSCI extends SerializationContextInitializer {
   TestDataSCI INSTANCE = new TestDataSCIImpl();
}
