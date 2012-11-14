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

import java.util.List;

import org.infinispan.remoting.transport.Address;

/**
 * As {@link DistributedTask} might potentially fail on subset of executing nodes FailureContext
 * provides details of such task failure. FailureContext has a scope of a node where the task
 * failed.
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface FailoverContext {

   /**
    * Returns an Address of the node where the task failed
    * 
    * @return the Address of the failed execution location
    */
   Address executionFailureLocation();

   /**
    * Returns a list of candidates for possible repeated execution governed by installed
    * {@link DistributedTaskFailoverPolicy}
    * 
    * @return an Address list of possible execution candidates
    */
   List<Address> executionCandidates();

   /**
    * Returns the Exception which was the cause of the task failure. This includes both system
    * exception related to Infinispan transient failures (node crash, transient errors etc) as well
    * as application level exceptions. Exception will contain the full chain of Exceptions. If
    * clients are interested in root cause of the Exception they can use appropriate
    * {@link Exception#getCause()} API to recursively traverse the Exception chain.
    * 
    * @return the Exception that caused task failure on the particular Infinispan node
    */
   Exception cause();

   /**
    * Returns a list of input keys for this task. Note that this method does not return all of the
    * keys used as input for {@link DistributedTask} but rather only the input keys used as input
    * for a part of that task where the execution failed
    * 
    * @param <K>
    * @return the list of input keys if any
    */
   <K> List<K> inputKeys();
}
