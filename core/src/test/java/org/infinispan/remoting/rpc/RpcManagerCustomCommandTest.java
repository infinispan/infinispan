/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.remoting.rpc;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "remoting.rpc.RpcManagerCustomCommandTest", groups = "functional")
public class RpcManagerCustomCommandTest extends MultipleCacheManagersTest {
   /**
    * Test to make sure that invokeRemotely returns the result from the remote
    * side.
    */
   public void testInvokeRemotely() {
      Cache<?, ?> cache = cache(0, "asyncRepl");
      AdvancedCache<?, ?> advancedCache = cache.getAdvancedCache();
      RpcManager manager = advancedCache.getRpcManager();
      Map<Address, Response> remoteResponses = manager.invokeRemotely(null, 
                  new CustomCommand(), true, true);
      
      assert 1 == remoteResponses.size() : "Expected only 1 remote response, got: " + remoteResponses;
      log.trace("Responses were: " + remoteResponses);
      Response response = remoteResponses.values().iterator().next();
      assert response != null : "Response was null!";
      assert response.isValid();
      assert response.isSuccessful();
      assert response instanceof SuccessfulResponse;
      Object value = ((SuccessfulResponse)response).getResponseValue();
      assert RETURN_VALUE.equals(value) : "Response was not what was expected, got: " + value;
   }
   
   /**
    * Test to make sure that invokeRemotely with a ResponseMode argument returns 
    * the result from the remote side
    */
   public void testInvokeRemotelyWithResponseMode() {
      Cache<?, ?> cache = cache(0, "asyncRepl");
      AdvancedCache<?, ?> advancedCache = cache.getAdvancedCache();
      RpcManager manager = advancedCache.getRpcManager();
      Map<Address, Response> remoteResponses = manager.invokeRemotely(null, 
                  new CustomCommand(), ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, 5000);
      
      assert 1 == remoteResponses.size() : "Expected only 1 remote response, got: " + remoteResponses;
      log.trace("Responses were: " + remoteResponses);
      Response response = remoteResponses.values().iterator().next();
      assert response != null : "Response was null!";
      assert response.isValid();
      assert response.isSuccessful();
      assert response instanceof SuccessfulResponse;
      Object value = ((SuccessfulResponse)response).getResponseValue();
      assert RETURN_VALUE.equals(value) : "Response was not what was expected, got: " + value;
   }
   
   private static final byte CUSTOM_COMMAND_ID = -127;
   private static final String RETURN_VALUE = "invoked";
   
   private static final class CustomCommand implements VisitableCommand, Serializable {

      private static final long serialVersionUID = -7404198907873296747L;

      @Override
      public Object perform(InvocationContext ctx) throws Throwable {
         return RETURN_VALUE;
      }

      @Override
      public byte getCommandId() {
         return CUSTOM_COMMAND_ID;
      }

      @Override
      public Object[] getParameters() {
         return new Object[0];
      }

      @Override
      public void setParameters(int commandId, Object[] parameters) {
      }

      @Override
      public boolean isReturnValueExpected() {
         return true;
      }

      @Override
      public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
         return visitor.visitUnknownCommand(ctx, this);
      }

      @Override
      public boolean shouldInvoke(InvocationContext ctx) {
         return true;
      }

      @Override
      public boolean ignoreCommandOnStatus(ComponentStatus status) {
         return false;
      }
   }
   
   private static final class CustomCommandFactory implements ExtendedModuleCommandFactory {
      @Override
      public Map<Byte, Class<? extends ReplicableCommand>> getModuleCommands() {
         Map<Byte, Class<? extends ReplicableCommand>> map = 
               new HashMap<Byte, Class<? extends ReplicableCommand>>();
         map.put(CUSTOM_COMMAND_ID, CustomCommand.class);
         return map;
      }

      @Override
      public ReplicableCommand fromStream(byte commandId, Object[] args) {
         ReplicableCommand c;
         switch (commandId) {
         case CUSTOM_COMMAND_ID:
            c = new CustomCommand();
            break;
         default:
            throw new IllegalArgumentException("Not registered to handle command id " + commandId);
         }
         c.setParameters(commandId, args);
         return c;
      }

      @Override
      public CacheRpcCommand fromStream(byte commandId, Object[] args, String cacheName) {
         return null;
      }
   }
   
   private void configureCustomFactoryManagers(int count) {
      for (int i = 0; i < count; ++i) {
         EmbeddedCacheManager manager = manager(i);
         GlobalComponentRegistry registry = (GlobalComponentRegistry) TestingUtil.extractField(manager, "globalComponentRegistry");
         registry.registerComponent(Collections.singletonMap(CUSTOM_COMMAND_ID, new CustomCommandFactory()), KnownComponentNames.MODULE_COMMAND_FACTORIES);
      }
   }
   
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(
            CacheMode.REPL_SYNC, true);
      final int managerCount = 2;
      createClusteredCaches(managerCount, "asyncRepl", builder);
      configureCustomFactoryManagers(managerCount);
   }
}
