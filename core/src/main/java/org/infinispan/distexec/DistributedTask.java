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

/**
 * DistributedTask describes all relevant attributes of a distributed task, most importantly its
 * execution policy, fail over policy and its timeout.
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface DistributedTask<T> {

   /**
    * Returns timeout for the execution of this task
    * 
    * @return task timeout
    */
   long timeout();

   /**
    * Returns custom {@link DistributedTaskExecutionPolicy} for this task
    * 
    * @return task DistributedTaskExecutionPolicy
    */
   DistributedTaskExecutionPolicy getTaskExecutionPolicy();

   /**
    * Returns {@link Callable} for this task
    * 
    * @return task callable 
    */
   Callable<T> getCallable();

}
