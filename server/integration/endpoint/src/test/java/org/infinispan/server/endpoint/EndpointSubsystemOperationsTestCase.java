package org.infinispan.server.endpoint;

import org.infinispan.server.endpoint.subsystem.EndpointExtension;
import org.jboss.as.clustering.infinispan.subsystem.InfinispanSubsystemDependenciesInitialization;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.subsystem.test.AbstractSubsystemTest;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;
import static org.infinispan.server.endpoint.subsystem.ModelKeys.CACHE_NAMES;
import static org.infinispan.server.endpoint.subsystem.ModelKeys.IGNORED_CACHES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author gustavonalle
 * @since 8.1
 */
public class EndpointSubsystemOperationsTestCase extends AbstractSubsystemTest {

   private KernelServices services;

   public EndpointSubsystemOperationsTestCase() {
      super(Constants.SUBSYSTEM_NAME, new EndpointExtension());
   }

   protected String getSubsystemXml() throws IOException {
      return readResource("endpoint-8.0.xml");
   }

   @Before
   public void setUp() throws Exception {
      services = createKernelServicesBuilder(new InfinispanSubsystemDependenciesInitialization())
              .setSubsystemXml(getSubsystemXml()).build();
   }

   private void executeOp(String operationName, String... parameters) {
      ModelNode op = new ModelNode();
      op.get(OP).set(operationName);
      op.get(ADDRESS).set(PathAddress.pathAddress(Constants.SUBSYSTEM_PATH).toModelNode());
      ModelNode cacheNames = op.get(CACHE_NAMES);
      stream(parameters).forEach(cacheNames::add);
      ModelNode result = services.executeOperation(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
   }

   @Test
   public void testIgnoreCaches() throws Exception {
      executeOp("ignore-cache-all-endpoints", "cacheA", "cacheB", "cacheC");
      assertCachesIgnored("cacheA", "cacheB", "cacheC");

      executeOp("unignore-cache-all-endpoints", "cacheA");
      assertCachesIgnored("cacheB", "cacheC");

      executeOp("unignore-cache-all-endpoints", "cacheB", "cacheC");
      assertCachesNotIgnored("cacheA", "cacheB", "cacheC");
   }

   private void assertCachesIgnored(String... caches) {
      assertCachesOnEndpoints(services.readWholeModel(), true, caches);
   }

   private void assertCachesNotIgnored(String... caches) {
      assertCachesOnEndpoints(services.readWholeModel(), false, caches);
   }

   private void assertCachesOnEndpoints(ModelNode allModel, boolean assertDisabled, String... caches) {
      ModelNode endPointRoot = allModel.get(SUBSYSTEM, Constants.SUBSYSTEM_NAME);

      Set<ModelNode> allConnectors = endPointRoot.asList().stream()
              .flatMap(mn -> mn.get(0).asList().stream())
              .collect(Collectors.toSet());

      allConnectors.forEach(connector -> {
         ModelNode ignoredCaches = connector.get(0).get(IGNORED_CACHES);
         if (!ignoredCaches.isDefined()) {
            assertTrue(!assertDisabled);
         } else {
            Set<String> ignoredCacheNames = ignoredCaches.asList().stream().map(ModelNode::asString).collect(Collectors.toSet());
            if (assertDisabled) {
               assertTrue(stream(caches).allMatch(ignoredCacheNames::contains));
            } else {
               assertTrue(stream(caches).noneMatch(ignoredCacheNames::contains));
            }
         }
      });
   }

}
