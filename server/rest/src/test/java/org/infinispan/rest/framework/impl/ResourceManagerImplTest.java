package org.infinispan.rest.framework.impl;

import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;

import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RegistrationException;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "rest.framework.ResourceManagerImplTest")
public class ResourceManagerImplTest {

   private ResourceManagerImpl resourceManager = new ResourceManagerImpl("root");

   @Test(expectedExceptions = RegistrationException.class, expectedExceptionsMessageRegExp = "ISPN.*Cannot register invocation 'HandlerB'.*")
   public void testShouldPreventOverlappingVariablePaths() {
      registerHandler("HandlerA", "/{pathA}");
      registerHandler("HandlerB", "/{pathB}");
   }

   public void testAllowRegistrationForDifferentMethods() {
      registerHandler("HandlerA", GET, "/root/a/{pathA}");
      registerHandler("HandlerB", POST, "/root/a/{pathA}");
   }

   @Test
   public void testLookupHandler() {
      registerHandler("CompaniesHandler", "/companies", "/companies/{company}", "/companies/{company}/{id}");
      registerHandler("StocksHandler", "/stocks/{stock}", "/stocks/{stock}/{currency}");
      registerHandler("DirectorsHandler", "/directors", "/directors/director", "/directors/director/{personId}");
      registerHandler("InfoHandler", "/info", "/info/jvm", "/info/jvm/{format}", "/info/jvm/threaddump", "/info/{format}/{encoding}");

      assertNull(resourceManager.lookupResource(GET, "/root/dummy"));

      assertNull(resourceManager.lookupResource(GET, "/root/stocks"));
      assertNull(resourceManager.lookupResource(GET, "/root/stocks/2/USD/1"));
      assertInvocation(resourceManager.lookupResource(GET, "/root/stocks/2"), "StocksHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/stocks/2/USD"), "StocksHandler");

      assertInvocation(resourceManager.lookupResource(GET, "/root/directors"), "DirectorsHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/directors/director"), "DirectorsHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/directors/director/John"), "DirectorsHandler");
      assertNull(resourceManager.lookupResource(GET, "/root/directors/1345"));
      assertNull(resourceManager.lookupResource(GET, "/root/directors/director/Tim/123"));

      assertInvocation(resourceManager.lookupResource(GET, "/root/companies"), "CompaniesHandler");

      assertInvocation(resourceManager.lookupResource(GET, "/root/info"), "InfoHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/info/jvm"), "InfoHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/info/jvm/json"), "InfoHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/info/json/zip"), "InfoHandler");
   }

   private void assertInvocation(LookupResult result, String name) {
      assertEquals(name, result.getInvocation().getName());
   }

   private void registerHandler(String handlerName, Method method, String... paths) {
      resourceManager.registerResource(() -> {
         Invocations.Builder builder = new Invocations.Builder();
         Arrays.stream(paths).forEach(p -> builder.invocation().method(method).path(p).name(handlerName).handleWith(restRequest -> null));
         return builder.create();
      });
   }


   private void registerHandler(String handlerName, String... paths) {
      resourceManager.registerResource(() -> {
         Invocations.Builder builder = new Invocations.Builder();
         Arrays.stream(paths).forEach(p -> builder.invocation().methods(GET, HEAD, POST).path(p).name(handlerName).handleWith(restRequest -> null));
         return builder.create();
      });
   }
}
