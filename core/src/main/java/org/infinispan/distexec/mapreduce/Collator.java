/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.distexec.mapreduce;

import org.infinispan.remoting.transport.Address;

/**
 * Collator coordinates results from Reducers executed on Infinispan cluster and assembles a final
 * result returned to an invoker of MapReduceTask.
 * 
 * 
 * @see MapReduceTask
 * @see Reducer
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public interface Collator<R> {

   /**
    * Collates all results added so far and returns result R to invoker of distributed task.
    * 
    * @return final result of distributed task computation
    */
   R collate();

   /**
    * Invoked by runtime every time reduced result R is received from executed Reducer on remote
    * nodes.
    * 
    * @param remoteNode
    *           address of the node where reduce phase occurred
    * @param remoteResult
    *           the result R of reduce phase
    */
   void reducedResultReceived(Address remoteNode, R remoteResult);
}
