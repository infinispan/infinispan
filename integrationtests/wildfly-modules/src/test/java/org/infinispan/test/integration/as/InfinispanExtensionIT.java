package org.infinispan.test.integration.as;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.INCLUDE_RUNTIME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.READ_RESOURCE_OPERATION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.arquillian.api.ContainerResource;
import org.jboss.as.arquillian.container.ManagementClient;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.dmr.ModelNode;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test the infinispan extension can be loaded in WFLY
 *
 * @author Ryan Emerson
 * @since 9.2
 */
@ApplicationScoped
@RunWith(Arquillian.class)
public class InfinispanExtensionIT {

   @Resource(lookup = "java:jboss/datagrid-infinispan/container/infinispan_container")
   EmbeddedCacheManager container;

   @Resource(lookup = "java:jboss/datagrid-infinispan/container/infinispan_container/cache/default")
   Cache cache;

   @ContainerResource("container.active-1")
   ManagementClient managementClient;

   @Deployment(name = "dep1", order = 1)
   public static Archive<?> dep1() {
      return createDeployment("dep1.war");
   }

   @Deployment(name = "dep2", order = 2)
   public static Archive<?> dep2() {
      return createDeployment("dep2.war");
   }

   private static Archive<?> createDeployment(String name) {
      return ShrinkWrap.create(WebArchive.class, name)
            .addClass(InfinispanExtensionIT.class)
            .addClass(ManagementClient.class)
            .addClass(ModelControllerClient.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", createDepString("org.infinispan", "org.infinispan.server.endpoint",
                  "org.jgroups.extension")).exportAsString();
      return new StringAsset(manifest);
   }

   private static String createDepString(String... dependencies) {
      StringBuilder sb = new StringBuilder();
      for (String dep : dependencies)
         sb.append(dep).append(":").append(Version.getModuleSlot()).append(" services,");
      return sb.toString();
   }

   @Test
   @OperateOnDeployment("dep1")
   public void testDep1() {
      assertNotNull(container);
      assertTrue(container.getCacheManagerConfiguration().isClustered());
      assertNotNull(cache);
      cache.put("1", 1);
      assertEquals(1, cache.get("1"));
   }

   @Test
   @OperateOnDeployment("dep2")
   public void testDep2() {
      assertNotNull(container);
      assertTrue(container.getCacheManagerConfiguration().isClustered());
      assertNotNull(cache);
      assertEquals(1, cache.get("1"));
   }

   @Test
   public void testAllEndpointsAreLoaded() throws IOException {
      assert managementClient != null;
      ModelControllerClient client = managementClient.getControllerClient();
      readEndpointResource("hotrod-connector", client);
      readEndpointResource("rest-connector", client);
   }

   private void readEndpointResource(String endpoint, ModelControllerClient client) throws IOException {
      final ModelNode operation = new ModelNode();
      operation.get(OP).set(READ_RESOURCE_OPERATION);
      operation.get(OP_ADDR).add("subsystem", "datagrid-infinispan-endpoint").add(endpoint, endpoint);
      operation.get(INCLUDE_RUNTIME).set(true);
      ModelNode result = client.execute(operation);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
   }
}
