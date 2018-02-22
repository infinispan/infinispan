/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.PathAddress.pathAddress;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.Map;

import org.infinispan.cli.interpreter.Interpreter;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

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
   public void execute(OperationContext context, ModelNode operation) {
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
            .getService(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));
      EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();

      if (cacheManager == null) {
         return null;
      }

      return cacheManager.getGlobalComponentRegistry().getComponent(Interpreter.class);
   }
}
