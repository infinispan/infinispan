package org.infinispan.it.endpoints;

import org.infinispan.client.hotrod.event.CustomEventLogListener;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      includeClasses = {
            CustomEventLogListener.CustomEvent.class,
            CustomKey.class,
            CryptoCurrency.class,
            EmbeddedRestHotRodTest.Person.class
      },
      schemaFileName = "test.endpoints.it.proto",
      schemaFilePath = "org/infinispan/test",
      schemaPackageName = EndpointITSCI.PACKAGE_NAME,
      service = false
)
public interface EndpointITSCI extends GeneratedSchema {
   String PACKAGE_NAME = "org.infinispan.test.endpoint.it";
   EndpointITSCI INSTANCE = new EndpointITSCIImpl();

   static String getFQN(Class<?> messageClass) {
      return PACKAGE_NAME + "." + messageClass.getSimpleName();
   }
}
