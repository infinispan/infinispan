/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
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
package org.infinispan.query;

import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

/**
 * Component to rebuild the indexes from the existing data.
 * This process starts by removing all existing indexes, and then a distributed
 * task is executed to rebuild the indexes. This task can take a long time to run,
 * depending on data size, used CacheLoaders, indexing complexity.
 * 
 * While reindexing is being performed queries should not be executed as they
 * will very likely miss many or all results.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
@MBean(objectName = "MassIndexer",
      description = "Component that rebuilds the index from the cached data")
public interface MassIndexer {

   //TODO Add more parameters here, like timeout when it will be implemented
   //(see ISPN-1313, and ISPN-1042 for task cancellation)
   @ManagedOperation(description = "Starts rebuilding the index")
   @Operation(displayName = "Rebuild index")
   void start();

}
