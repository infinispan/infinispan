package org.infinispan.remoting.rpc;

import org.infinispan.marshall.protostream.impl.GlobalContextInitializer;
import org.infinispan.marshall.persistence.impl.PersistenceContextInitializer;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      dependsOn = {
            PersistenceContextInitializer.class,
            GlobalContextInitializer.class
      },
      includeClasses = {
            CustomCacheRpcCommand.class,
            CustomReplicableCommand.class,
            SleepingCacheRpcCommand.class
      },
      schemaFileName = "test.core.RpcSCI.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.core.RpcSCI",
      service = false
)
interface RpcSCI extends SerializationContextInitializer {
   SerializationContextInitializer INSTANCE = new RpcSCIImpl();
}
