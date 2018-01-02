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

import java.util.Arrays;

import org.jboss.as.clustering.infinispan.InfinispanMessages;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;

/**
 * Common code for handling the following cache configuration elements
 * {locking, transaction, eviction, expiration, state-transfer, rehashing, store, file-store, jdbc-store, remote-store}
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author William Burns (c) 2013 Red Hat Inc.
 */
class CacheConfigOperationHandlers {
    static final OperationStepHandler CONTAINER_CONFIGURATIONS_ADD = new ReloadRequiredAddStepHandler();
    static final OperationStepHandler CONTAINER_SECURITY_ADD = new ReloadRequiredAddStepHandler();

    static final OperationStepHandler LOADER_ADD = new CacheLoaderAdd();
    static final OperationStepHandler LOADER_PROPERTY_ADD = new ReloadRequiredAddStepHandler(new AttributeDefinition[]{LoaderPropertyResource.VALUE});
    static final OperationStepHandler CLUSTER_LOADER_ADD = new ClusterCacheLoaderAdd();
    static final OperationStepHandler STORE_ADD = new CacheStoreAdd();
    static final OperationStepHandler STORE_WRITE_BEHIND_ADD = new ReloadRequiredAddStepHandler(StoreWriteBehindResource.ATTRIBUTES);
    static final OperationStepHandler FILE_STORE_ADD = new FileCacheStoreAdd();
    static final OperationStepHandler STRING_KEYED_JDBC_STORE_ADD = new StringKeyedJDBCCacheStoreAdd();
    static final OperationStepHandler REMOTE_STORE_ADD = new RemoteCacheStoreAdd();
    static final OperationStepHandler ROCKSDB_STORE_ADD = new RocksDBCacheStoreAdd();
    static final OperationStepHandler ROCKSDB_EXPIRATION_ADD = new ReloadRequiredAddStepHandler(RocksDBExpirationConfigurationResource.ATTRIBUTES);
    static final OperationStepHandler LEVELDB_COMPRESSION_ADD = new ReloadRequiredAddStepHandler(RocksDBCompressionConfigurationResource.ATTRIBUTES);
    static final OperationStepHandler REST_STORE_ADD = new RestCacheStoreAdd();

    /**
     * Base class for adding cache loaders.
     *
     * This class needs to do the following:
     * - check that its parent has no existing defined cache loader
     * - process its model attributes
     * - create any child resources required for the loader resource, such as a set of properties
     *
     */
    abstract static class AbstractCacheLoaderAdd extends AbstractAddStepHandler {
        protected final AttributeDefinition[] attributes;

        AbstractCacheLoaderAdd(AttributeDefinition[] attributes) {
           this(attributes, false);
        }

        /**
        * @param attributes The attributes or additional attributes to use
        * @param includeCommonLoaderAttributes Loader implementations should provide false for this
        */
        AbstractCacheLoaderAdd(AttributeDefinition[] attributes, boolean includeCommonLoaderAttributes) {
           if (includeCommonLoaderAttributes) {
              this.attributes = concat(BaseLoaderConfigurationResource.BASE_LOADER_PARAMETERS, attributes);
           }
           else {
              this.attributes = attributes;
           }
        }

        @Override
        protected void populateModel(OperationContext context, ModelNode operation, Resource resource) throws OperationFailedException {
            final ModelNode model = resource.getModel();

            // Process attributes
            for(final AttributeDefinition attribute : attributes) {
                // we use PROPERTIES only to allow the user to pass in a list of properties on store add commands
                // don't copy these into the model
                if (attribute.getName().equals(BaseStoreConfigurationResource.PROPERTIES.getName()))
                    continue ;
                attribute.validateAndSet(operation, model);
            }

            // Process type specific properties if required
            populateSubclassModel(context, operation, model);

            // The cache config parameters  <property name=>value</property>
            if(operation.hasDefined(ModelKeys.PROPERTIES)) {
                // CLI will be a list where there is N elements each with a single map
                // RHQ will be a list with 1 element where each element is a map with N elements
                // Thus we have to iterate this way instead of just doing asPropertyList for example from the PROPERTIES
                // ModelNode returned from the operation
                for(ModelNode node : operation.get(ModelKeys.PROPERTIES).asList()) {
                    for (Property property : node.asPropertyList()) {
                        // create a new property=name resource
                        final Resource param = context.createResource(PathAddress.pathAddress(PathElement.pathElement(ModelKeys.PROPERTY, property.getName())));
                        final ModelNode value = property.getValue();
                        if(! value.isDefined()) {
                            throw InfinispanMessages.MESSAGES.propertyValueNotDefined(property.getName());
                        }
                        ModelNode holder = new ModelNode();
                        holder.get(LoaderPropertyResource.VALUE.getName()).set(value);
                        // set the value of the property
                        LoaderPropertyResource.VALUE.validateAndSet(holder, param.getModel());
                    }
                }
            }
        }

        void populateSubclassModel(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
            // this abstract method is called when populateModel() is called in the base class
            for(final AttributeDefinition attribute : attributes) {
                attribute.validateAndSet(operation, model);
            }
        }

        @Override
        protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
            // do nothing
        }
    }

    /**
     * Add a basic cache loader to a cache.
     */
    private static class CacheLoaderAdd extends AbstractCacheLoaderAdd {

        CacheLoaderAdd() {
            super(LoaderConfigurationResource.LOADER_ATTRIBUTES, true);
        }
    }

    /**
     * Add a cluster cache loader to a cache.
     */
    private static class ClusterCacheLoaderAdd extends AbstractCacheLoaderAdd {
        ClusterCacheLoaderAdd() {
            super(ClusterLoaderConfigurationResource.ATTRIBUTES, true);
        }
    }

    /**
     * Base class for adding cache stores.
     *
     * This class needs to do the following:
     * - check that its parent has no existing defined cache store
     * - process its model attributes
     * - create any child resources required for the store resource, such as a set of properties
     *
     */
    abstract static class AbstractCacheStoreAdd extends AbstractCacheLoaderAdd {
        AbstractCacheStoreAdd() {
            super(BaseStoreConfigurationResource.BASE_STORE_PARAMETERS);
        }

        AbstractCacheStoreAdd(AttributeDefinition[] attributes) {
            super(concat(BaseStoreConfigurationResource.BASE_STORE_PARAMETERS, attributes));
        }
    }

   private static AttributeDefinition[] concat(AttributeDefinition[] A, AttributeDefinition[] B) {
      int aLen = A.length;
      int bLen = B.length;
      AttributeDefinition[] C;
      if (bLen > 0) {
         C = Arrays.copyOf(A, aLen + bLen);
         System.arraycopy(B, 0, C, aLen, bLen);
      }
      else {
         C = A;
      }
      return C;
   }

    /**
     * Add a basic cache store to a cache.
     */
    private static class CacheStoreAdd extends AbstractCacheStoreAdd {
        CacheStoreAdd() {
            super(StoreConfigurationResource.STORE_ATTRIBUTES);
        }
    }

    /**
     * Add a file cache store to a cache.
     */
    private static class FileCacheStoreAdd extends AbstractCacheStoreAdd {
        FileCacheStoreAdd() {
            super(FileStoreResource.ATTRIBUTES);
        }
    }

    private static class JDBCCacheStoreAdd extends AbstractCacheStoreAdd {
        JDBCCacheStoreAdd() {
            super(BaseJDBCStoreConfigurationResource.COMMON_JDBC_STORE_ATTRIBUTES);
        }

    }

    private static class StringKeyedJDBCCacheStoreAdd extends JDBCCacheStoreAdd {
        private final AttributeDefinition[] additionalAttributes;

        StringKeyedJDBCCacheStoreAdd() {
            this.additionalAttributes = StringKeyedJDBCStoreResource.ATTRIBUTES;
        }

        @Override
        protected void populateSubclassModel(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
            // this abstract method is called when populateModel() is called in the base class
            super.populateSubclassModel(context, operation, model);

            for(final AttributeDefinition attribute : additionalAttributes) {
                attribute.validateAndSet(operation, model);
            }
            // now check for string-keyed-table passed as optional parameter, in order to create the resource
//            if (operation.get("string-keyed-table").isDefined()) {
//                ModelNode stringTable = operation.get("string-keyed-table") ;
//                // process this table DMR description
//            }
        }
    }

    private static class RemoteCacheStoreAdd extends AbstractCacheStoreAdd {
        RemoteCacheStoreAdd() {
            super(RemoteStoreConfigurationResource.REMOTE_STORE_ATTRIBUTES);
        }
    }

    private static class RocksDBCacheStoreAdd extends AbstractCacheStoreAdd {
        RocksDBCacheStoreAdd() {
            super(RocksDBStoreConfigurationResource.ROCKSDB_STORE_ATTRIBUTES);
        }
    }

    private static class RestCacheStoreAdd extends AbstractCacheStoreAdd {
        RestCacheStoreAdd() {
            super(RestStoreConfigurationResource.REST_STORE_ATTRIBUTES);
        }
    }
}
