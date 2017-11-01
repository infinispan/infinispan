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

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PartitionHandlingConfigurationBuilder;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.partitionhandling.PartitionHandling;
import org.jboss.as.clustering.infinispan.conflict.DeployedMergePolicy;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;

/**
 * @author Paul Ferraro
 * @author Tristan Tarrant
 */
public abstract class SharedStateCacheConfigurationAdd extends ClusteredCacheConfigurationAdd {

    SharedStateCacheConfigurationAdd(CacheMode mode) {
        super(mode);
    }

    @Override
    void processModelNode(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {

        // process the basic clustered configuration
        super.processModelNode(context, containerName, cache, builder, dependencies);

        // state transfer is a child resource
        if (cache.hasDefined(ModelKeys.STATE_TRANSFER) && cache.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME).isDefined()) {
            ModelNode stateTransfer = cache.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);

            final boolean enabled = StateTransferConfigurationResource.ENABLED.resolveModelAttribute(context, stateTransfer).asBoolean();
            final boolean awaitInitialTransfer = StateTransferConfigurationResource.AWAIT_INITIAL_TRANSFER.resolveModelAttribute(context,  stateTransfer).asBoolean();
            final long timeout = StateTransferConfigurationResource.TIMEOUT.resolveModelAttribute(context, stateTransfer).asLong();
            final int chunkSize = StateTransferConfigurationResource.CHUNK_SIZE.resolveModelAttribute(context, stateTransfer).asInt();

            builder.clustering().stateTransfer().fetchInMemoryState(enabled);
            builder.clustering().stateTransfer().awaitInitialTransfer(awaitInitialTransfer);
            builder.clustering().stateTransfer().timeout(timeout);
            builder.clustering().stateTransfer().chunkSize(chunkSize);
        }

       if (cache.hasDefined(ModelKeys.PARTITION_HANDLING) && cache.get(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME).isDefined()) {
          ModelNode partitionHandling = cache.get(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME);

          PartitionHandlingConfigurationBuilder phBuilder = builder.clustering().partitionHandling();
          phBuilder.enabled(PartitionHandlingConfigurationResource.ENABLED.resolveModelAttribute(context, partitionHandling).asBoolean());

          String phType = PartitionHandlingConfigurationResource.WHEN_SPLIT.resolveModelAttribute(context, partitionHandling).asString();
          if (phType != null)
            phBuilder.whenSplit(PartitionHandling.valueOf(phType));

          String policyClassName = PartitionHandlingConfigurationResource.MERGE_POLICY.resolveModelAttribute(context, partitionHandling).asString();
          Parser.MergePolicy mergePolicy = Parser.MergePolicy.fromString(policyClassName);
          EntryMergePolicy policy = mergePolicy != Parser.MergePolicy.CUSTOM ? mergePolicy.getImpl() : new DeployedMergePolicy(policyClassName);
          phBuilder.mergePolicy(policy);
       }
    }
}
