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

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

/**
 * @author Paul Ferraro
 * @author Tristan Tarrant
 */
public class BackupSiteResource extends CacheChildResource {

    static final AttributeDefinition[] ATTRIBUTES = new AttributeDefinition[] { };

    // operations
    static final OperationDefinition BACKUP_BRING_SITE_ONLINE =
            new SimpleOperationDefinitionBuilder("bring-site-online", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    static final OperationDefinition BACKUP_TAKE_SITE_OFFLINE =
            new SimpleOperationDefinitionBuilder("take-site-offline", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    static final OperationDefinition BACKUP_SITE_STATUS =
            new SimpleOperationDefinitionBuilder("site-status", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    static final OperationDefinition BACKUP_PUSH_STATE =
            new SimpleOperationDefinitionBuilder("push-state", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    static final OperationDefinition BACKUP_CANCEL_PUSH_STATE =
            new SimpleOperationDefinitionBuilder("cancel-push", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    static final OperationDefinition BACKUP_CANCEL_RECEIVE_STATE =
            new SimpleOperationDefinitionBuilder("cancel-receive", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    static final OperationDefinition BACKUP_PUSH_STATE_STATUS =
            new SimpleOperationDefinitionBuilder("push-state-status", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    static final OperationDefinition BACKUP_CLEAR_PUSH_STATE_STATUS =
            new SimpleOperationDefinitionBuilder("clear-push-state-status", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    static final OperationDefinition BACKUP_SENDING_SITE =
            new SimpleOperationDefinitionBuilder("get-sending-site", new InfinispanResourceDescriptionResolver("backup.ops"))
                .setRuntimeOnly().build();

    BackupSiteResource(CacheResource cacheResource) {
        super(PathElement.pathElement(ModelKeys.BACKUP), ModelKeys.BACKUP, cacheResource, ATTRIBUTES);
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        if (resource.isRuntimeRegistration()) {
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_BRING_SITE_ONLINE, CacheCommands.BackupBringSiteOnlineCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_TAKE_SITE_OFFLINE, CacheCommands.BackupTakeSiteOfflineCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_SITE_STATUS, CacheCommands.BackupSiteStatusCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_PUSH_STATE, CacheCommands.BackupPushStateCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_CANCEL_PUSH_STATE, CacheCommands.BackupCancelPushStateCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_CANCEL_RECEIVE_STATE, CacheCommands.BackupCancelReceiveStateCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_PUSH_STATE_STATUS, CacheCommands.BackupPushStateStatusCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_CLEAR_PUSH_STATE_STATUS, CacheCommands.BackupClearPushStatusCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(BackupSiteResource.BACKUP_SENDING_SITE, CacheCommands.BackupGetSendingSiteCommand.INSTANCE);
        }
    }
}
