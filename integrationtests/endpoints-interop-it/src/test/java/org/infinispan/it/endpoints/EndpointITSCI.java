package org.infinispan.it.endpoints;

import org.infinispan.client.hotrod.event.CustomEventLogListener;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = {
            CustomEventLogListener.CustomEvent.class,
            CustomKey.class,
            CryptoCurrency.class,
            EmbeddedRestHotRodTest.Person.class
      },
      schemaFileName = "test.endpoints.it.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = EndpointITSCI.PACKAGE_NAME,
      service = false
)
public interface EndpointITSCI extends SerializationContextInitializer {
   String PACKAGE_NAME = "org.infinispan.test.endpoint.it";
   EndpointITSCI INSTANCE = new EndpointITSCIImpl();

   static String getFQN(Class<?> messageClass) {
      return PACKAGE_NAME + "." + messageClass.getSimpleName();
   }
}
