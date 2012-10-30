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
package org.infinispan.distexec.mapreduce;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;


import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;

/**
 * MapReduceManager is an internal Infinispan component receiving map/reduce invocations arriving
 * from remote Infinispan nodes.
 * <p>
 * 
 * This interface should never be implemented by clients.
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface MapReduceManager {

   /**
    * Invoked when MapCombineCommand arrives to a target Infinispan node and returns set of
    * intermediate keys that needs to be reduced.
    * 
    * @param mcc MapCombineCommand sent from MapReduceTask
    * @return a set of intermediate keys to be reduced distributively 
    */
   <KIn, VIn, KOut, VOut> Set<KOut> mapAndCombineForDistributedReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException;

   /**
    * Invoked when MapCombineCommand arrives to a target Infinispan node and returns map of resulting 
    * values that need to be reduced.
    * <p>
    * Unlike {@link #mapAndCombineForDistributedReduction(MapCombineCommand)} 
    * implementations should return a map Map<KOut, List<VOut>> which should be ready for reduction
    * at master Infinispan node
    * <p>
    * 
    * @param mcc MapCombineCommand sent from MapReduceTask
    * @return a map Map<KOut, List<VOut>> which should be ready for reduction
    * at master Infinispan node
    */
   <KIn, VIn, KOut, VOut> Map<KOut, List<VOut>> mapAndCombineForLocalReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException;

   /**
    * Invoked when ReduceCommand arrives to a target Infinispan node. Implementations should return
    * a map of reduced output keys and values to be returned to invoker of MapReduceTask
    * 
    * @param reducer ReduceCommand sent from MapReduceTask
    * @return map of reduced output keys and values returned to MapReduceTask
    */
   <KOut, VOut> Map<KOut, VOut> reduce(ReduceCommand<KOut, VOut> reducer) throws InterruptedException;
   
   /**
    * Maps Map/Reduce task intermediate or input keys to nodes on Infinispan cluster
    * 
    * 
    * @param dm
    *           distribution manager to use for locating keys on hash wheel
    * @param taskId
    *           id of the map/reduce task
    * @param keysToMap
    *           list of input keys to locate in the cluster
    * @param useIntermediateCompositeKey
    *           if true use composite keys for shared intermediate cache
    * @return map where each key is an Address in the cluster and value are the keys mapped to that
    *         Address
    */
   <T> Map<Address, List<T>> mapKeysToNodes(DistributionManager dm, String taskId,
            Collection<T> keysToMap, boolean useIntermediateCompositeKey);
   
   /**
    * ExecutorService provided for local task execution
    * 
    * @return {@link ExecutorService} for local tasks
    */
   ExecutorService getExecutorService();

}
