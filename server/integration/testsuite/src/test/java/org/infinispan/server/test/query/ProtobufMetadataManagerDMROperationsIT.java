package org.infinispan.server.test.query;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.RESULT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;
import static org.junit.Assert.assertEquals;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.clustering.infinispan.subsystem.InfinispanExtension;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.dmr.ModelNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Exercise all DMR ops exposed for ProtobufMetadataManager.
 *
 * @author anistor@redhat.com
 */
@Category(Queries.class)
@RunWith(Arquillian.class)
public class ProtobufMetadataManagerDMROperationsIT {

   private static final String containerName = "clustered";

   @InfinispanResource("remote-query-1")
   protected RemoteInfinispanServer server;

   private ModelControllerClient controller;

   @Before
   public void setUp() throws Exception {
      controller = ModelControllerClient.Factory.create(
            server.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
   }

   @After
   public void tearDown() throws Exception {
      try {
         if (controller != null) {
            controller.close();
         }
      } finally {
         RemoteCacheManagerFactory rcmFactory = new RemoteCacheManagerFactory();
         ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
         clientBuilder.addServer()
               .host(server.getHotrodEndpoint().getInetAddress().getHostName())
               .port(server.getHotrodEndpoint().getPort())
               .marshaller(new ProtoStreamMarshaller());
         RemoteCacheManager remoteCacheManager = rcmFactory.createManager(clientBuilder);
         RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
         metadataCache.remove("test1.proto");
         metadataCache.remove("test2.proto");
         metadataCache.remove("test3.proto");
         rcmFactory.stopManagers();
      }
   }

   @Test
   public void testOperations() throws Exception {
      // get all schema names
      ModelNode op = getOperation("get-proto-schema-names");
      ModelNode result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());

      // unregister all schemas
      op = getOperation("unregister-proto-schemas");
      op.get("file-names").set(result.get(RESULT).asList());
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());

      // ensure there are no schemas
      op = getOperation("get-proto-schema-names");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[]", result.get(RESULT).asString());

      // ensure there are no errors
      op = getOperation("get-proto-schemas-with-errors");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[]", result.get(RESULT).asString());

      // register a valid schema file
      op = getOperation("register-proto-schemas");
      op.get("file-names").set(new ModelNode().add("test1.proto"));
      op.get("file-contents").set(new ModelNode().add("package test;"));
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());

      // ensure the schema is defined
      op = getOperation("get-proto-schema-names");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[\"test1.proto\"]", result.get(RESULT).asString());

      // ensure there are no errors
      op = getOperation("get-proto-schemas-with-errors");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[]", result.get(RESULT).asString());

      // check the contents of test1.proto
      op = getOperation("get-proto-schema");
      op.get("file-name").set("test1.proto");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("package test;", result.get(RESULT).asString());

      // register a valid schema file by uploading it from classpath
      op = getOperation("upload-proto-schemas");
      op.get("file-names").set(new ModelNode().add("test2.proto"));
      ModelNode urlList = new ModelNode().add(getClass().getResource("/sample_bank_account/bank.proto").toString());
      op.get("file-urls").set(urlList);
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());

      // ensure the schema is defined
      op = getOperation("get-proto-schema-names");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[\"test1.proto\",\"test2.proto\"]", result.get(RESULT).asString());

      // ensure there are no errors
      op = getOperation("get-proto-schemas-with-errors");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[]", result.get(RESULT).asString());

      // register an invalid schema file
      op = getOperation("register-proto-schemas");
      op.get("file-names").set(new ModelNode().add("test3.proto"));
      op.get("file-contents").set(new ModelNode().add("kabooom"));
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());

      // ensure the schema is defined
      op = getOperation("get-proto-schema-names");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[\"test1.proto\",\"test2.proto\",\"test3.proto\"]", result.get(RESULT).asString());

      // check the contents of test3.proto
      op = getOperation("get-proto-schema");
      op.get("file-name").set("test3.proto");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("kabooom", result.get(RESULT).asString());

      // ensure test3.proto has errors
      op = getOperation("get-proto-schemas-with-errors");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[\"test3.proto\"]", result.get(RESULT).asString());

      // check there are errors in test3.proto
      op = getOperation("get-proto-schema-errors");
      op.get("file-name").set("test3.proto");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("java.lang.IllegalStateException: Syntax error in test3.proto at 1:8: unexpected label: kabooom", result.get(RESULT).asString());

      // unregister test3.proto
      op = getOperation("unregister-proto-schemas");
      op.get("file-names").set(new ModelNode().add("test3.proto"));
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());

      // check the are no more errors in test3.proto
      op = getOperation("get-proto-schema-errors");
      op.get("file-name").set("test3.proto");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("undefined", result.get(RESULT).asString());

      // ensure there are no errors
      op = getOperation("get-proto-schemas-with-errors");
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
      assertEquals("[]", result.get(RESULT).asString());

      // unregister test1.proto and test2.proto
      op = getOperation("unregister-proto-schemas");
      op.get("file-names").set(new ModelNode().add("test1.proto").add("test2.proto"));
      result = controller.execute(op);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());
   }

   private ModelNode getOperation(String operationName) {
      PathAddress address = PathAddress.pathAddress(
            PathElement.pathElement(ModelDescriptionConstants.SUBSYSTEM, InfinispanExtension.SUBSYSTEM_NAME))
            .append("cache-container", containerName);
      ModelNode op = new ModelNode();
      op.get(OP).set(operationName);
      op.get(OP_ADDR).set(address.toModelNode());
      return op;
   }
}
