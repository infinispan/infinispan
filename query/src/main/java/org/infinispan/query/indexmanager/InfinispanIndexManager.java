/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.query.indexmanager;

import java.util.Properties;

import org.hibernate.search.backend.BackendFactory;
import org.hibernate.search.backend.spi.BackendQueueProcessor;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.hibernate.search.spi.WorkerBuildContext;
import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.search.store.impl.DirectoryProviderFactory;

/**
 * A custom IndexManager to store indexes in the grid itself.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class InfinispanIndexManager extends DirectoryBasedIndexManager {

   private InfinispanCommandsBackend remoteMaster;

   protected BackendQueueProcessor createBackend(String indexName, Properties cfg, WorkerBuildContext buildContext) {
      BackendQueueProcessor localMaster = BackendFactory.createBackend(this, buildContext, cfg);
      remoteMaster = new InfinispanCommandsBackend();
      remoteMaster.initialize(cfg, buildContext, this);
      //localMaster is already initialized by the BackendFactory
      MasterSwitchDelegatingQueueProcessor joinedMaster = new MasterSwitchDelegatingQueueProcessor(localMaster, remoteMaster);
      return joinedMaster;
   }

   protected DirectoryProvider createDirectoryProvider(String indexName, Properties cfg, WorkerBuildContext buildContext) {
      return DirectoryProviderFactory.createDirectoryProvider(indexName, cfg, buildContext);
   }

   public InfinispanCommandsBackend getRemoteMaster() {
      return remoteMaster;
   }

}
