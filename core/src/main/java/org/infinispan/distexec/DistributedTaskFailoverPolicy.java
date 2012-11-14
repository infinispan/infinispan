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
    * As parts of distributively executed task can fail due to the task itself throwing an exception
    * or it can be an Infinispan system caused failure (e.g node failed or left cluster during task
    * execution etc).
    * 
    * @param failoverContext
    *           the FailoverContext of the failed execution
    * @return result the Address of the Infinispan node selected for fail over execution
    */
   Address failover(FailoverContext context);

   /**
    * Maximum number of fail over attempts permitted by this DistributedTaskFailoverPolicy
    * 
    * @return max number of fail over attempts
    */
   int maxFailoverAttempts();
}
