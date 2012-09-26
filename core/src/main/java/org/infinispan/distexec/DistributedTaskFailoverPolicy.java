/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.distexec;

import java.util.List;

import org.infinispan.remoting.transport.Address;

/**
 * DistributedTaskFailoverPolicy allows pluggable fail over target selection for a failed remotely
 * executed distributed task.
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface DistributedTaskFailoverPolicy {

   /**
    * 
    * As parts of distributively executed task can fail due to the task itself throwing an exception
    * or it can be an Infinispan system caused failure (e.g node failed or left cluster during task
    * execution). Either way, the given exception along with the given list of available execution
    * candidates should be used for a possible fail-over and execution of a failed task to another
    * Infinispan node.
    * 
    * @param failedExecution
    *           the Address of the node where execution of task failed
    * @param executionCandidates
    *           the list of nodes available for re-attempted execution of distributed task
    * @param cause
    *           the Exception capturing details of a failed task execution
    * @return result the Address of the Infinispan node selected for fail over execution
    */
   Address failover(Address failedExecution, List<Address> executionCandidates, Exception cause);

   /**
    * Maximum number of fail over attempts permitted by this DistributedTaskFailoverPolicy
    * 
    * @return max number of fail over attempts
    */
   int maxFailoverAttempts();
}
