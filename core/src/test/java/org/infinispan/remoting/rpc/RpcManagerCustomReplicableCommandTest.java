package org.infinispan.remoting.rpc;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author William Burns
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(testName = "remoting.rpc.RpcManagerCustomReplicableCommandTest", groups = "functional")
public class RpcManagerCustomReplicableCommandTest extends MultipleCacheManagersTest {

   protected static final String TEST_CACHE = "testCache";

   protected static final String EXPECTED_RETURN_VALUE = "the-return-value";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      createClusteredCaches(2, TEST_CACHE, RpcSCI.INSTANCE, builder);
   }

   protected ReplicableCommand createReplicableCommandForTest(Object arg) {
      return new CustomReplicableCommand(arg);
   }

   /**
    * Test to make sure that invokeRemotely returns the result from the remote side.
    */
   public void testInvokeRemotely() {
      RpcManager rpcManager = cache(0, "testCache").getAdvancedCache().getRpcManager();
      ReplicableCommand command = createReplicableCommandForTest(EXPECTED_RETURN_VALUE);
      Map<Address, Response> remoteResponses = invoke(rpcManager, command);
      log.tracef("Responses were: %s", remoteResponses);

      assertEquals(1, remoteResponses.size());
      Response response = remoteResponses.values().iterator().next();
      assertNotNull(response);
      assertTrue(response.isValid());
      assertTrue(response.isSuccessful());
      assertTrue(response instanceof SuccessfulResponse);
      Object value = ((SuccessfulResponse) response).getResponseValue();
      assertEquals(EXPECTED_RETURN_VALUE, value);
   }

   /**
    * Test to make sure that invokeRemotely results in a RemoteException.
    */
   public void testInvokeRemotelyWithRemoteException() {
      RpcManager rpcManager = cache(0, "testCache").getAdvancedCache().getRpcManager();
      ReplicableCommand command = createReplicableCommandForTest(new IllegalArgumentException("exception!"));

      try {
         invoke(rpcManager, command);
         fail("Expected RemoteException not thrown");
      } catch (RemoteException e) {
         assertTrue(e.getCause() instanceof IllegalArgumentException);
         assertEquals("exception!", e.getCause().getMessage());
      } catch (Exception ex) {
         fail("Expected exception not thrown but instead we got : " + ex);
      }
   }

   /**
    * Test to make sure that invokeRemotely with a ResponseMode argument returns the result from the remote side.
    */
   public void testInvokeRemotelyWithResponseMode() {
      RpcManager rpcManager = cache(0, "testCache").getAdvancedCache().getRpcManager();
      ReplicableCommand command = createReplicableCommandForTest(EXPECTED_RETURN_VALUE);

      Map<Address, Response> remoteResponses = invoke(rpcManager, command);
      log.tracef("Responses were: %s", remoteResponses);

      assertEquals(1, remoteResponses.size());
      Response response = remoteResponses.values().iterator().next();
      assertNotNull(response);
      assertTrue(response.isValid());
      assertTrue(response.isSuccessful());
      assertTrue(response instanceof SuccessfulResponse);
      Object value = ((SuccessfulResponse) response).getResponseValue();
      assertEquals(EXPECTED_RETURN_VALUE, value);
   }

   /**
    * Test to make sure that invokeRemotely with a ResponseMode argument returns the result from the remote side.
    */
   public void testInvokeRemotelyWithResponseModeWithRemoteException() {
      RpcManager rpcManager = cache(0, "testCache").getAdvancedCache().getRpcManager();
      ReplicableCommand command = createReplicableCommandForTest(new IllegalArgumentException("exception!"));

      try {
         invoke(rpcManager, command);
         fail("Expected RemoteException not thrown");
      } catch (RemoteException e) {
         assertTrue(e.getCause() instanceof IllegalArgumentException);
         assertEquals("exception!", e.getCause().getMessage());
      } catch (Exception ex) {
         fail("Expected exception not thrown but instead we got : " + ex);
      }
   }

   private Map<Address, Response> invoke(RpcManager rpcManager, ReplicableCommand command) {
      return rpcManager.blocking(
            rpcManager.invokeCommandOnAll(command, MapResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions())
      );
   }
}
