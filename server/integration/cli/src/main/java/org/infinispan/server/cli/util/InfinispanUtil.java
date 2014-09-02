package org.infinispan.server.cli.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.server.cli.CliInterpreterException;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandFormatException;
import org.jboss.as.cli.CommandLineException;
import org.jboss.as.cli.Util;
import org.jboss.as.cli.operation.OperationFormatException;
import org.jboss.as.cli.operation.OperationRequestAddress;
import org.jboss.as.cli.operation.impl.DefaultCallbackHandler;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.dmr.ModelNode;


/**
 * Some utility methods.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class InfinispanUtil {

   private static final String SUBSYSTEM = "subsystem";
   private static final String INFINISPAN = "infinispan";
   private static final String INFINISPAN_SUBSYSTEM = "/" + SUBSYSTEM + "=" + INFINISPAN;
   private static final String CONTAINER_TYPE = "cache-container";
   private static final String CONTAINER_ADDRESS = INFINISPAN_SUBSYSTEM + "/" + CONTAINER_TYPE;

   private InfinispanUtil() {
   }

   public static List<String> getContainerNames(CommandContext ctx) throws OperationFormatException {
      OperationRequestAddress address = buildOperationRequest(ctx, INFINISPAN_SUBSYSTEM);
      return Util.getNodeNames(ctx.getModelControllerClient(), address, CONTAINER_TYPE);
   }

   public static boolean containsContainer(CommandContext ctx, String container) throws OperationFormatException {
      return new HashSet<String>(getContainerNames(ctx)).contains(container);
   }

   public static void changeToContainer(CommandContext ctx, String container) throws CommandLineException {
      if (!containsContainer(ctx, container)) {
         throw new CommandLineException("No such container: " + container);
      }
      OperationRequestAddress address = ctx.getCurrentNodePath();
      address.reset();
      address.toNode("subsystem", "infinispan");
      address.toNode(CONTAINER_TYPE, container);
   }

   public static Map<String, List<String>> getCachesNames(CommandContext ctx, String container) throws CommandLineException {
      if (!containsContainer(ctx, container)) {
         throw new CommandLineException("No such container: " + container);
      }
      OperationRequestAddress address = buildOperationRequest(ctx, CONTAINER_ADDRESS + "=" + container);
      List<String> cacheModes = Util.getNodeTypes(ctx.getModelControllerClient(), address);
      Map<String, List<String>> caches = new HashMap<String, List<String>>();
      for (String cacheMode : cacheModes) {
         caches.put(cacheMode, Util.getNodeNames(ctx.getModelControllerClient(), address, cacheMode));
      }
      return caches;
   }

   public static void changeToCache(CommandContext ctx, String container, String cache) throws CommandLineException {
      Map<String, List<String>> caches = getCachesNames(ctx, container);
      for (Map.Entry<String, List<String>> entry : caches.entrySet()) {
         if (entry.getValue().contains(cache)) {
            changeToContainer(ctx, container);
            OperationRequestAddress address = ctx.getCurrentNodePath();
            address.toNode(entry.getKey(), cache);
            return;
         }
      }
   }

   public static void connect(CommandContext ctx, String container, String cache) throws CommandLineException {
      if (container == null) {
         List<String> containers = getContainerNames(ctx);
         if (containers.isEmpty()) {
            throw new CommandLineException("No containers found!");
         }
         container = containers.get(0);
      }
      try {
         if (cache != null) {
            changeToCache(ctx, container, cache);
            cliRequest(ctx, "cache " + cache + "\n");
         } else {
            changeToContainer(ctx, container);
         }
      } catch (CommandLineException e) {
         ctx.getCurrentNodePath().reset();
         throw e;
      } catch (CliInterpreterException e) {
         ctx.getCurrentNodePath().reset();
         throw new CommandLineException(e.getLocalizedMessage());
      }
   }

   public static void disconnect(CommandContext ctx) {
      ctx.getCurrentNodePath().reset();
   }

   public static ModelNode cliRequest(CommandContext ctx, String command) throws CommandLineException, CliInterpreterException {
      final ModelNode request = buildCliRequest(ctx, command);

      final ModelControllerClient client = ctx.getModelControllerClient();
      final ModelNode response;
      try {
         response = client.execute(request);
      } catch (Exception e) {
         throw new CommandFormatException("Failed to perform operation: " + e.getLocalizedMessage());
      }
      if (!Util.isSuccess(response)) {
         throw new CommandFormatException(Util.getFailureDescription(response));
      }
      if (!response.has(Util.RESULT)) {
         return null;
      }
      ModelNode result = response.get(Util.RESULT);
      if (Boolean.parseBoolean(result.get("isError").asString())) {
         throw new CliInterpreterException(result.get("result").asString());
      }
      updateStateFromResponse(result, ctx);
      return result;
   }

   public static ModelNode buildCliRequest(CommandContext ctx, String command) throws CommandLineException {
      final CacheInfo cacheInfo = getCacheInfo(ctx);
      OperationRequestAddress requestAddress = getContainerAddress(ctx, cacheInfo.getContainer());
      ModelNode req = Util.buildRequest(ctx, requestAddress, "cli-interpreter");
      updateRequest(req, ctx, command, cacheInfo);
      return req;
   }

   public static void updateStateFromResponse(ModelNode node, CommandContext context) {
      if (node.has("sessionId")) {
         context.set(getCacheInfo(context).getContainer() + "-sessionId", node.get("sessionId").asString());
      }
   }

   public static OperationRequestAddress buildOperationRequest(CommandContext ctx, String address) throws OperationFormatException {
      DefaultCallbackHandler handler = new DefaultCallbackHandler();
      ctx.getCommandLineParser().parse(address, handler);
      return handler.getAddress();
   }

   /**
    * @return the container and cache name.
    */
   public static CacheInfo getCacheInfo(CommandContext context) {
      String container = null;
      String cache = null;
      final Iterator<OperationRequestAddress.Node> iterator = context.getCurrentNodePath().iterator();
      for (int i = 0; i < 3 && iterator.hasNext(); ++i) {
         OperationRequestAddress.Node node = iterator.next();
         if (node.getType().equals(CONTAINER_TYPE)) {
            container = node.getName();
         } else if (node.getType().endsWith("-cache")) {
            cache = node.getName();
         }
      }
      return new CacheInfo(container, cache);
   }

   private static void updateRequest(ModelNode request, CommandContext context, String command, CacheInfo cacheInfo) {
      setInModelNode(request, "cacheName", cacheInfo.getCache());
      setInModelNode(request, "sessionId", (String) context.get(cacheInfo.getContainer() + "-sessionId"));
      setInModelNode(request, "command", command);
   }

   private static void setInModelNode(ModelNode node, String key, String value) {
      if (value != null) {
         node.get(key).set(value);
      }
   }

   private static OperationRequestAddress getContainerAddress(CommandContext ctx, String container) throws CommandLineException {
      if (container == null) {
         throw new CommandLineException("Container does not exists");
      }
      return buildOperationRequest(ctx, CONTAINER_ADDRESS + "=" + container);
   }

   public static class CacheInfo {
      private final String container;
      private final String cache;

      public CacheInfo(String container, String cache) {
         this.container = container;
         this.cache = cache;
      }

      public String getContainer() {
         return container;
      }

      public String getCache() {
         return cache;
      }
   }

}
