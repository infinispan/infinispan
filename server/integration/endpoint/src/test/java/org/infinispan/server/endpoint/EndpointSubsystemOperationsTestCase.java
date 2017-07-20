package org.infinispan.server.endpoint;

import static java.util.Arrays.stream;
import static org.infinispan.server.endpoint.subsystem.ModelKeys.CACHE_NAMES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADDRESS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.RESULT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.infinispan.server.endpoint.subsystem.EndpointExtension;
import org.jboss.as.clustering.infinispan.subsystem.InfinispanSubsystemDependenciesInitialization;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.subsystem.test.AbstractSubsystemTest;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;
import org.junit.Before;
import org.junit.Test;

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
      return readResource("endpoint-9.2.xml");
   }

   @Before
   public void setUp() throws Exception {
      services = createKernelServicesBuilder(new InfinispanSubsystemDependenciesInitialization())
              .setSubsystemXml(getSubsystemXml()).build();
   }

   private ModelNode executeOp(String operationName, String... parameters) {
      ModelNode op = new ModelNode();
      op.get(OP).set(operationName);
      op.get(ADDRESS).set(PathAddress.pathAddress(Constants.SUBSYSTEM_PATH).toModelNode());
      ModelNode cacheNames = op.get(CACHE_NAMES);
      stream(parameters).forEach(cacheNames::add);
      ModelNode result = services.executeOperation(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      return result.get(RESULT);
   }

   @Test
   public void testIgnoreCaches() throws Exception {
      assertCachesNotIgnored("cache1", "cache2", "cache3");
      assertCachesNotIgnored("whatever");

      executeOp("ignore-cache-all-endpoints", "cacheA", "cacheB", "cacheC");
      assertCachesIgnored("cacheA", "cacheB", "cacheC");

      executeOp("unignore-cache-all-endpoints", "cacheA");
      assertCachesNotIgnored("cacheA");
      assertCachesIgnored("cacheB", "cacheC");

      executeOp("unignore-cache-all-endpoints", "cacheB");
      assertCachesNotIgnored("cacheA", "cacheB");
      assertCachesNotIgnored("cacheC");

      executeOp("unignore-cache-all-endpoints", "cacheA", "cacheB", "cacheC");
      assertCachesNotIgnored("cacheA", "cacheB", "cacheC");
   }

   private void assertCachesIgnored(String... caches) {
      assertCacheStatus(true, caches);
   }

   private void assertCachesNotIgnored(String... caches) {
      assertCacheStatus(false, caches);
   }

   private void assertCacheStatus(boolean ignored, String... caches) {
      ModelNode summary = executeOp("is-ignored-all-endpoints", caches);
      stream(caches).allMatch(cache -> summary.get(cache).asBoolean() == ignored);
   }

}
