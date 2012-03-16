/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import java.util.Map;

/**
 * Collator collates results from Reducers executed on Infinispan cluster and assembles a final
 * result returned to an invoker of MapReduceTask.
 * 
 * 
 * @see MapReduceTask#execute(Collator)
 * @see MapReduceTask#executeAsynchronously(Collator)
 * @see Reducer
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public interface Collator<KOut, VOut, R> {

   /**
    * Collates all reduced results and returns R to invoker of distributed task.
    * 
    * @return final result of distributed task computation
    */
   R collate(Map<KOut, VOut> reducedResults);
}
