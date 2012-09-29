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
package org.infinispan.distexec;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * DistributedTaskBuilder is a factory interface for DistributedTask
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface DistributedTaskBuilder<T> {

   /**
    * Provide relevant {@link Callable} for the {@link DistributedTask}
    * 
    * @param callable
    *           for the DistribtuedTask being built
    * @return this DistributedTaskBuilder
    */
   DistributedTaskBuilder<T> callable(Callable<T> callable);

   /**
    * Provide {@link DistributedTask} task timeout
    * 
    * @param timeout
    *           for the task
    * @param tu
    *           {@link TimeUnit} for the task being built
    * @return this DistributedTaskBuilder
    */
   DistributedTaskBuilder<T> timeout(long timeout, TimeUnit tu);

   /**
    * Provide {@link DistributedTaskExecutionPolicy} for the task being built
    * 
    * @param policy
    *           DistributedTaskExecutionPolicy for the task
    * @return this DistributedTaskBuilder
    */
   DistributedTaskBuilder<T> executionPolicy(DistributedTaskExecutionPolicy policy);
   
   /**
    * Provide {@link DistributedTaskFailoverPolicy} for the task being built
    * 
    * @param policy
    *           DistributedTaskFailoverPolicy for the task
    * @return this DistributedTaskBuilder
    */
   DistributedTaskBuilder<T> failoverPolicy(DistributedTaskFailoverPolicy policy);

   /**
    * Completes creation of DistributedTask with the currently provided attributes of this
    * DistributedTaskBuilder
    * 
    * @return the built task ready for use
    */
   DistributedTask<T> build();
}
