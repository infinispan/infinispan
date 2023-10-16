package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * This test ensures when using a server that a backup response does not carry the previous value with
 * it in its message, which would increase intra cluster traffic for no reason.
 */
@Test(groups = {"functional"}, testName = "distribution.DistBackupWriteResponseTest")
public class DistServerBackupWriteResponseTest extends BaseDistFunctionalTest<Object, String> {
   @Override
   protected void createClusteredCache() {
      SerializationContextInitializer sci = getSerializationContext();
      GlobalConfigurationBuilder globalBuilder = defaultGlobalConfigurationBuilder();
      if (sci != null) globalBuilder.serialization().addContextInitializer(sci);
      // Make sure it is OBJECT so it uses the MagicKey hashCode as server mode would serialize it
      configuration.encoding().mediaType(MediaType.APPLICATION_OBJECT);
      // Create clustered caches with failure detection protocols on
      createClusteredCaches(INIT_CLUSTER_SIZE, globalBuilder, configuration, true,
            new TransportFlags().withFD(false), cacheName);
   }

   public void testRemoveBackupResponseContainsNoValues() {
      MagicKey key = getKeyForCache(c1, c2);
      c1.put(key, "value");

      EmbeddedCacheManager cm = manager(0);

      JGroupsTransport actualTransport = (JGroupsTransport) TestingUtil.extractGlobalComponent(cm, Transport.class);

      RequestRepository requestRepository = TestingUtil.extractField(actualTransport, "requests");

      RequestRepository spy = Mockito.spy(requestRepository);

      TestingUtil.replaceField(spy, "requests", actualTransport, JGroupsTransport.class);

      assertEquals("value", c1.remove(key));

      ArgumentCaptor<Response> responseCaptor = ArgumentCaptor.forClass(Response.class);
      Mockito.verify(spy).addResponse(Mockito.anyLong(), Mockito.any(), responseCaptor.capture());

      Response response = responseCaptor.getValue();
      assertTrue(response instanceof SuccessfulResponse);

      assertNull(((SuccessfulResponse<?>) response).getResponseValue());
   }
}
