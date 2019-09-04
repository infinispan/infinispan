package org.infinispan.marshall.protostream.impl;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.marshall.persistence.impl.PersistenceContextInitializerImpl;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.UserMarshallerBytesSizeTest")
public class UserMarshallerBytesSizeTest extends AbstractInfinispanTest {

   public void correctSizeCalculatedTest() throws Exception {
      SerializationContext ctx = ProtobufUtil.newSerializationContext();
      SerializationContextInitializer sci = new PersistenceContextInitializerImpl();
      sci.registerSchema(ctx);
      sci.registerMarshallers(ctx);

      byte[] userBytes = new byte[10];
      byte[] wrappedMessage = ProtobufUtil.toWrappedByteArray(ctx, new UserMarshallerBytes(userBytes));
      assertEquals(wrappedMessage.length, UserMarshallerBytes.size(userBytes.length));
   }
}
