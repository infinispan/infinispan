/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.distexec.mapreduce;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Reduces intermediate key/value results from map phase of MapReduceTask. Infinispan distributed
 * execution environment uses one instance of Reducer per execution node.
 * 
 * 
 * @see Mapper
 * @see MapReduceTask
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 * 
 * @since 5.0
 */
public interface Reducer<KOut, VOut> extends Serializable {

   /**
    * Combines/reduces all intermediate values for a particular intermediate key to a single value.
    * <p>
    * 
    */
   VOut reduce(KOut reducedKey, Iterator<VOut> iter);

}
