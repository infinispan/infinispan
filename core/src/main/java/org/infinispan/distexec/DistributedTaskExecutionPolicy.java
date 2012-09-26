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
import java.util.Map;

import org.infinispan.remoting.transport.Address;

/**
 * DistributedTaskExecutionPolicy allows task to specify its custom task execution policy across
 * Infinispan cluster. Users of {@link DistributedExecutorService} thus effectively load balance
 * distributed tasks, veto and override Infinispan nodes selected for task execution.
 * 
 * For example, someone might want to execute tasks exclusively on a local network centre instead of
 * a backup remote network centre. Others might, for example, use only a dedicated subset of nodes
 * in Infinispan cluster for task execution. DistributedTaskExecutionPolicy is set per instance of
 * DistributedTask.
 * 
 * 
 * Infinispan's DistributedExecutorService always invokes executionTargetSelected method while
 * mapKeysToExecutionNodes is invoked only if input keys are given. If you are dealing with tasks
 * that have input keys all you need to do is to implement mapKeysToExecution method, otherwise, if
 * no keys are used, the second method executionTargetSelected needs to be implemented. Infinispan's
 * DistributedExecutorService implementation, upon task submittal, first invokes
 * mapKeysToExecutionNode method callback, if and only if input keys are given. After task mapping
 * phase DistributedExecutorService invokes executionTargetSelected callback regardless if input
 * keys are given or not.
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface DistributedTaskExecutionPolicy {

   /**
    * Given a set of input keys for a distributed task an implementation should return an address to
    * list of keys mapping where each Address key is a node to execute a distributed task T and list
    * of keys value is a list of input keys for distributed task T.
    * 
    * @param <K>
    *           the type for input keys
    * @param input
    *           all input keys for a distributed task
    * @return an address to list of keys mapping
    * 
    */
   <K> Map<Address, List<K>> keysToExecutionNodes(K... keys);

   /**
    * Implementations of DistributedTaskMappingPolicy are given an opportunity to override suggested
    * execution node with a another node.
    * <p>
    * 
    * 
    * @param executionTarget
    *           execution target suggested
    * @param candidates
    *           list of potential candidates to choose from
    * @return an address of the node that is a actually selected for task execution
    */
   Address executionTargetSelected(Address executionTarget, List<Address> candidates);

}
