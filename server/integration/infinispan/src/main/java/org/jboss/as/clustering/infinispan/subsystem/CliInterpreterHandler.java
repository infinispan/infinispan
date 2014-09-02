package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.cli.interpreter.Interpreter;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.SecurityActions;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

import java.util.Map;

import static org.jboss.as.controller.PathAddress.pathAddress;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * CLI operation handler. This is registered in {@link org.infinispan.manager.CacheContainer} and it forwards the
 * commands to the CLI interpreter.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class CliInterpreterHandler implements OperationStepHandler {
   public static final CliInterpreterHandler INSTANCE = new CliInterpreterHandler();

   @Override
   public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
      final ModelNode result = new ModelNode();
      try {
         final String command = operation.require("command").asString();
         final String cacheName = operation.has("cacheName") ? operation.get("cacheName").asString() : null;
         String sessionId = operation.has("sessionId") ? operation.get("sessionId").asString() : null;
         final Interpreter interpreter = getInterpreter(context, operation);

         if (interpreter == null) {
            context.getFailureDescription().set("Interpreter not found!");
            context.getResult().set(result);
            return;
         }

         if (sessionId == null) {
            sessionId = interpreter.createSessionId(cacheName);
            setInModelNode(result, "sessionId", sessionId);
         }

         final Map<String, String> response = SecurityActions.executeInterpreter(interpreter, sessionId, command);

         setResponse(result, response);
         context.getResult().set(result);
      } catch (Exception e) {
         e.printStackTrace();
         context.getFailureDescription().set(e.getLocalizedMessage());
         context.getResult().set(result);
      } finally {
         context.stepCompleted();

      }
   }

   private void setResponse(ModelNode node, Map<String, String> response) {
      setInModelNode(node, "cacheName", response.get(ResultKeys.CACHE.toString()));
      setInModelNode(node, "result", response.get(ResultKeys.OUTPUT.toString()));
      setInModelNode(node, "result", response.get(ResultKeys.ERROR.toString()));
      setInModelNode(node, "isError", Boolean.toString(response.get(ResultKeys.ERROR.toString()) != null));
   }

   private static void setInModelNode(ModelNode node, String key, String value) {
      if (value != null) {
         node.get(key).set(value);
      }
   }

   private Interpreter getInterpreter(OperationContext context, ModelNode operation) {
      final PathAddress address = pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getLastElement().getValue();
      final ServiceController<?> controller = context.getServiceRegistry(false)
            .getService(EmbeddedCacheManagerService.getServiceName(cacheContainerName));
      EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();

      if (cacheManager == null) {
         return null;
      }

      return cacheManager.getGlobalComponentRegistry().getComponent(Interpreter.class);
   }
}
