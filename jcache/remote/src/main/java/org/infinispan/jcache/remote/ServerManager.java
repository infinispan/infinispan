package org.infinispan.jcache.remote;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.client.ModelControllerClient;
import static org.jboss.as.controller.client.helpers.ClientConstants.*;
import org.jboss.dmr.ModelNode;

public class ServerManager {
   private static final String INFINISPAN_SUBSYSTEM_NAME = "datagrid-infinispan";
   private static final String INFINISPAN_ENDPOINT_SUBSYSTEM_NAME = "datagrid-infinispan-endpoint";
   private String host;
   private int port;

   public ServerManager(String host) {
      this(host, 9990);
   }

   public ServerManager(String host, int port) {
      this.host = host;
      this.port = port;
   }

   public Set<String> getCacheNames() throws NotAvailableException, ManagementClientException {
      checkServerManagementAvailable();

      final Set<String> result = new HashSet<String>();

      withManagementClient(host, port, new ManagementRunnable() {
         @Override
         public void run(ModelControllerClient client) throws Exception {
            PathAddress pathAddress = PathAddress.pathAddress(SUBSYSTEM, INFINISPAN_SUBSYSTEM_NAME)
                  .append("cache-container", getHotRodCacheContainer(client));

            ModelNode op = new ModelNode();
            op.get(OP).set(READ_CHILDREN_NAMES_OPERATION);
            op.get(OP_ADDR).set(pathAddress.toModelNode());
            op.get(CHILD_TYPE).set("local-cache");

            ModelNode resp = client.execute(op);
            if (!SUCCESS.equals(resp.get(OUTCOME).asString())) {
               throw new IllegalArgumentException(resp.asString());
            }
            if (!resp.has(RESULT)) {
               return;
            }
            List<ModelNode> modelNodes = resp.get(RESULT).asList();
            for (ModelNode modelNode : modelNodes) {
               result.add(modelNode.asString());
            }
         }
      });

      return result;
   }

   private void checkExistence(final String cacheType, final String cacheName, final AtomicBoolean result) throws ManagementClientException {
      if (!result.get()) {
         withManagementClient(host, port, new ManagementRunnable() {
            @Override
            public void run(ModelControllerClient client) throws Exception {
               PathAddress pathAddress = PathAddress.pathAddress(SUBSYSTEM, INFINISPAN_SUBSYSTEM_NAME)
                     .append("cache-container", getHotRodCacheContainer(client))
                     .append(cacheType, cacheName);

               ModelNode op = new ModelNode();
               op.get(OP).set(READ_ATTRIBUTE_OPERATION);
               op.get(OP_ADDR).set(pathAddress.toModelNode());
               op.get(NAME).set("configuration");

               ModelNode resp = client.execute(op);
               if (SUCCESS.equals(resp.get(OUTCOME).asString())) {
                  result.set(true);
               }
            }
         });
      }
   }

   public boolean containsCache(final String cacheName) throws NotAvailableException, ManagementClientException {
      checkServerManagementAvailable();

      final AtomicBoolean result = new AtomicBoolean(false);

      checkExistence("local-cache", cacheName, result);
      checkExistence("distributed-cache", cacheName, result);
      checkExistence("replicated-cache", cacheName, result);
      checkExistence("invalidation-cache", cacheName, result);

      return result.get();
   }

   public void addCache(final String cacheName) throws NotAvailableException, ManagementClientException {
      checkServerManagementAvailable();

      withManagementClient(host, port, new ManagementRunnable() {
         @Override
         public void run(ModelControllerClient client) throws Exception {
            final ModelNode composite = new ModelNode();
            composite.get(OP).set(COMPOSITE);
            composite.get(OP_ADDR).setEmptyList();

            final ModelNode steps = composite.get(STEPS);

            final ModelNode configAddOp = steps.add();
            configAddOp.get(OP).set(ADD);
            String cacheContainer = getHotRodCacheContainer(client);
            configAddOp.get(OP_ADDR).set(PathAddress.pathAddress(SUBSYSTEM, INFINISPAN_SUBSYSTEM_NAME)
                  .append("cache-container", cacheContainer)
                  .append("configurations", "CONFIGURATIONS")
                  .append("local-cache-configuration", cacheName).toModelNode());
            configAddOp.get("template").set(false);

            final ModelNode cacheAddOp = steps.add();
            cacheAddOp.get(OP).set(ADD);
            cacheAddOp.get(OP_ADDR).set(PathAddress.pathAddress(SUBSYSTEM, INFINISPAN_SUBSYSTEM_NAME)
                  .append("cache-container", cacheContainer)
                  .append("local-cache", cacheName).toModelNode());
            cacheAddOp.get("configuration").set(cacheName);

            ModelNode resp = client.execute(composite);
            if (!SUCCESS.equals(resp.get(OUTCOME).asString())) {
               System.err.println(resp);
               throw new IllegalArgumentException(resp.asString());
            }
         }
      });
   }

   public void removeCache(final String cacheName) throws NotAvailableException, ManagementClientException {
      checkServerManagementAvailable();

      withManagementClient(host, port, new ManagementRunnable() {
         @Override
         public void run(ModelControllerClient client) throws Exception {
            final ModelNode composite = new ModelNode();
            composite.get(OP).set(COMPOSITE);
            composite.get(OP_ADDR).setEmptyList();

            final ModelNode steps = composite.get(STEPS);

            final ModelNode cacheRemoveOp = steps.add();
            cacheRemoveOp.get(OP).set(REMOVE_OPERATION);
            String cacheContainer = getHotRodCacheContainer(client);
            cacheRemoveOp.get(OP_ADDR).set(PathAddress.pathAddress(SUBSYSTEM, INFINISPAN_SUBSYSTEM_NAME)
                  .append("cache-container", cacheContainer)
                  .append("local-cache", cacheName).toModelNode());
            final ModelNode configRemoveOp = steps.add();
            configRemoveOp.get(OP).set(REMOVE_OPERATION);
            configRemoveOp.get(OP_ADDR).set(PathAddress.pathAddress(SUBSYSTEM, INFINISPAN_SUBSYSTEM_NAME)
                  .append("cache-container", cacheContainer)
                  .append("configurations", "CONFIGURATIONS")
                  .append("local-cache-configuration", cacheName).toModelNode());

            ModelNode resp = client.execute(composite);
            if (!SUCCESS.equals(resp.get(OUTCOME).asString())) {
               //TODO: don't ignore failures other than "resource doens't exist"
               System.err.println(resp);
            }
         }
      });
   }

   private String getHotRodCacheContainer(ModelControllerClient client) throws IOException {
      PathAddress pathAddress = PathAddress.pathAddress(SUBSYSTEM, INFINISPAN_ENDPOINT_SUBSYSTEM_NAME)
            .append("hotrod-connector", "hotrod-connector");

      ModelNode op = new ModelNode();
      op.get(OP).set(READ_ATTRIBUTE_OPERATION);
      op.get(OP_ADDR).set(pathAddress.toModelNode());
      op.get("name").set("cache-container");

      ModelNode resp = client.execute(op);
      if (!SUCCESS.equals(resp.get(OUTCOME).asString())) {
         throw new IllegalArgumentException(resp.asString());
      }
      return resp.get(RESULT).asString();
   }

   private static void withManagementClient(String host, int port, ManagementRunnable runnable) throws ManagementClientException {
      InetAddress addr;
      try {
         addr = InetAddress.getByName(host);
      } catch (UnknownHostException ex) {
         throw new ManagementClientException(String.format("Failed to resolve host '%s'.", host), ex);
      }
      ModelControllerClient client = ModelControllerClient.Factory.create("http-remoting", addr, port);
      try {
         runnable.run(client);
      } catch (Exception ex) {
         throw new ManagementClientException("", ex);
      } finally {
         try {
            client.close();
         } catch (IOException e) {
            // Ignore
         }
      }
   }

   /**
    * wildfly-controller is an optional dependency
    *
    * @throws NotAvailableException
    */
   private static void checkServerManagementAvailable() throws NotAvailableException {
      try {
         boolean skipServerMgmtLookup = Boolean.parseBoolean(System.getProperty("infinispan.jcache.mgmt.lookup.skip", "false"));
         if (skipServerMgmtLookup) {
            throw new NotAvailableException();
         }
         Class.forName("org.jboss.as.controller.client.ModelControllerClient");
      } catch (ClassNotFoundException e) {
         throw new NotAvailableException();
      }
   }

   private interface ManagementRunnable {
      void run(ModelControllerClient client) throws Exception;
   }

   public static class NotAvailableException extends Exception {
      private static final long serialVersionUID = 2036495722939416728L;

      public NotAvailableException() {
      }

      public NotAvailableException(String message, Throwable cause) {
         super(message, cause);
      }
   }

   public static class ManagementClientException extends Exception {
      private static final long serialVersionUID = -5857650491476258495L;

      public ManagementClientException() {
      }

      public ManagementClientException(String message, Throwable cause) {
         super(message, cause);
      }
   }
}
