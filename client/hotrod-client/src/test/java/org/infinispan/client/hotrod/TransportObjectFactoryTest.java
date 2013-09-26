package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TransportObjectFactory;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static org.jgroups.util.Util.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "unit", testName = "client.hotrod.TransportObjectFactoryTest")
public class TransportObjectFactoryTest {

   public void testValidate() {
      Codec codec = mock(Codec.class);
      TransportObjectFactory objectFactory = new TransportObjectFactory(codec, null,
                                                                        new AtomicInteger(), false);
      doThrow(new TransportException("induced!", null))
            .when(codec).writeHeader(any(Transport.class), any(HeaderParams.class));

      InetSocketAddress address = new InetSocketAddress(123);
      assertFalse("Exception shouldn't be thrown here", objectFactory.validateObject(address, any(TcpTransport.class)));
   }
}
