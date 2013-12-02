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

import org.infinispan.transaction.LockingMode;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/transaction=TRANSACTION
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class TransactionResource extends SimpleResourceDefinition {

    public static final PathElement TRANSACTION_PATH = PathElement.pathElement(ModelKeys.TRANSACTION, ModelKeys.TRANSACTION_NAME);

    // attributes
    // cache mode required, txn mode not
    static final SimpleAttributeDefinition LOCKING =
            new SimpleAttributeDefinitionBuilder(ModelKeys.LOCKING, ModelType.STRING, true)
                    .setXmlName(Attribute.LOCKING.getLocalName())
                    .setAllowExpression(true)
                    .setValidator(new EnumValidator<LockingMode>(LockingMode.class, true, false))
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(LockingMode.OPTIMISTIC.name()))
                    .build();
    static final SimpleAttributeDefinition MODE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MODE, ModelType.STRING, true)
                    .setXmlName(Attribute.MODE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setValidator(new EnumValidator<TransactionMode>(TransactionMode.class, true, true))
                    .setDefaultValue(new ModelNode().set(TransactionMode.NONE.name()))
                    .build();
    static final SimpleAttributeDefinition STOP_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.STOP_TIMEOUT, ModelType.LONG, true)
                    .setXmlName(Attribute.STOP_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(30000))
                    .build();

    static final AttributeDefinition[] TRANSACTION_ATTRIBUTES = {MODE, STOP_TIMEOUT, LOCKING};

 // operation parameters
    static final SimpleAttributeDefinition TX_INTERNAL_ID =
            new SimpleAttributeDefinitionBuilder(ModelKeys.TX_INTERNAL_ID, ModelType.LONG, true)
                .setXmlName(ModelKeys.TX_INTERNAL_ID)
                .setAllowExpression(false)
                .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                .build();

    // operations
    static final OperationDefinition RESET_TX_STATISTICS =
            new SimpleOperationDefinitionBuilder("reset-transaction-statistics", InfinispanExtension.getResourceDescriptionResolver("transaction"))
                .build();
    static final OperationDefinition LIST_IN_DOUBT_TRANSACTIONS =
            new SimpleOperationDefinitionBuilder("list-in-doubt-transactions", InfinispanExtension.getResourceDescriptionResolver("transaction"))
                .build();
    static final OperationDefinition TRANSACTION_FORCE_COMMIT =
            new SimpleOperationDefinitionBuilder("force-commit-transaction", InfinispanExtension.getResourceDescriptionResolver("transaction.recovery"))
                .addParameter(TX_INTERNAL_ID)
                .build();
    static final OperationDefinition TRANSACTION_FORCE_ROLLBACK =
            new SimpleOperationDefinitionBuilder("force-rollback-transaction", InfinispanExtension.getResourceDescriptionResolver("transaction.recovery"))
                .addParameter(TX_INTERNAL_ID)
                .build();
    static final OperationDefinition TRANSACTION_FORGET =
            new SimpleOperationDefinitionBuilder("forget-transaction", InfinispanExtension.getResourceDescriptionResolver("transaction.recovery"))
                .addParameter(TX_INTERNAL_ID)
                .build();

    private final boolean runtimeRegistration;

    public TransactionResource(boolean runtimeRegistration) {
        super(TRANSACTION_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.TRANSACTION),
                CacheConfigOperationHandlers.TRANSACTION_ADD,
                ReloadRequiredRemoveStepHandler.INSTANCE);
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // check that we don't need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(TRANSACTION_ATTRIBUTES);
        for (AttributeDefinition attr : TRANSACTION_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        if (runtimeRegistration) {
            resourceRegistration.registerOperationHandler(TransactionResource.RESET_TX_STATISTICS, CacheCommands.ResetTxStatisticsCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(TransactionResource.LIST_IN_DOUBT_TRANSACTIONS, CacheCommands.TransactionListInDoubtCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(TransactionResource.TRANSACTION_FORCE_COMMIT, CacheCommands.TransactionForceCommitCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(TransactionResource.TRANSACTION_FORCE_ROLLBACK, CacheCommands.TransactionForceRollbackCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(TransactionResource.TRANSACTION_FORGET, CacheCommands.TransactionForgetCommand.INSTANCE);
        }

    }
}
