/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
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

import static org.jboss.as.clustering.infinispan.InfinispanLogger.ROOT_LOGGER;

import org.infinispan.server.infinispan.task.ServerTaskProcessor;
import org.infinispan.server.infinispan.task.ServerTaskRegistryService;
import org.jboss.as.clustering.infinispan.conflict.DeployedMergePolicyFactoryService;
import org.jboss.as.clustering.infinispan.conflict.DeployedMergePolicyProcessor;
import org.jboss.as.clustering.infinispan.cs.deployment.AdvancedCacheLoaderExtensionProcessor;
import org.jboss.as.clustering.infinispan.cs.deployment.AdvancedCacheWriterExtensionProcessor;
import org.jboss.as.clustering.infinispan.cs.deployment.AdvancedLoadWriteStoreExtensionProcessor;
import org.jboss.as.clustering.infinispan.cs.deployment.CacheLoaderExtensionProcessor;
import org.jboss.as.clustering.infinispan.cs.deployment.CacheWriterExtensionProcessor;
import org.jboss.as.clustering.infinispan.cs.deployment.ExternalStoreExtensionProcessor;
import org.jboss.as.clustering.infinispan.cs.deployment.ServerExtensionDependenciesProcessor;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactoryService;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.server.AbstractDeploymentChainStep;
import org.jboss.as.server.DeploymentProcessorTarget;
import org.jboss.as.server.deployment.Phase;
import org.jboss.dmr.ModelNode;

/**
 * @author Paul Ferraro
 */
public class InfinispanSubsystemAdd extends AbstractAddStepHandler {

   private static final int POST_MODULE_PRIORITY = 0x1300;
   private static final int DEPENDENCIES_PRIORITY_PRIORITY = 0x1300;

   private static final String INFINISPAN_SUBSYSTEM_NAME = "datagrid-infinispan";

   private static void populate(ModelNode source, ModelNode target) {
      target.get(ModelKeys.CACHE_CONTAINER).setEmptyObject();
   }

   @Override
   protected void populateModel(ModelNode operation, ModelNode model) {
      populate(operation, model);
   }

   @Override
   protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) {
      ROOT_LOGGER.activatingSubsystem();
      addDeployableExtensionProcessors(context);
   }

   private void addDeployableExtensionProcessors(OperationContext context) {
      context.addStep(new AbstractDeploymentChainStep() {
         @Override
        protected void execute(DeploymentProcessorTarget processorTarget) {
            int basePriority = POST_MODULE_PRIORITY;
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.POST_MODULE, ++basePriority, new AdvancedCacheLoaderExtensionProcessor());
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.POST_MODULE, ++basePriority, new AdvancedCacheWriterExtensionProcessor());
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.POST_MODULE, ++basePriority, new AdvancedLoadWriteStoreExtensionProcessor());
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.POST_MODULE, ++basePriority, new CacheLoaderExtensionProcessor());
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.POST_MODULE, ++basePriority, new CacheWriterExtensionProcessor());
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.POST_MODULE, ++basePriority, new ExternalStoreExtensionProcessor());
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.POST_MODULE, ++basePriority, new ServerTaskProcessor());
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.POST_MODULE, ++basePriority, new DeployedMergePolicyProcessor());
            processorTarget.addDeploymentProcessor(INFINISPAN_SUBSYSTEM_NAME, Phase.DEPENDENCIES, DEPENDENCIES_PRIORITY_PRIORITY, new ServerExtensionDependenciesProcessor());
         }
      }, OperationContext.Stage.RUNTIME);

      context.getServiceTarget().addService(DeployedCacheStoreFactoryService.SERVICE_NAME, new DeployedCacheStoreFactoryService()).install();
      context.getServiceTarget().addService(ServerTaskRegistryService.SERVICE_NAME, new ServerTaskRegistryService()).install();
      context.getServiceTarget().addService(DeployedMergePolicyFactoryService.SERVICE_NAME, new DeployedMergePolicyFactoryService()).install();
   }
}
