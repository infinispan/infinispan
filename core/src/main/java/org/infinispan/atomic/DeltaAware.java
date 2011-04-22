/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.atomic;

/**
 * This interface allows the extraction of {@link Delta}s.
 * <br/>
 * Implementations would be closely coupled to a corresponding {@link Delta} implementation, since {@link
 * Delta#merge(DeltaAware)}  would need to know how to recreate this instance of DeltaAware if needed.
 * <br/>
 * Implementations of DeltaAware automatically gain the ability to perform fine-grained replication in Infinispan,
 * since Infinispan's data container is able to detect these types and only serialize and transport Deltas around
 * the network rather than the entire, serialized object.
 * <br />
 * Using DeltaAware makes sense if your custom object is large in size and often only sees small portions of the
 * object being updated in a transaction.  Implementations would need to be able to track these changes during the
 * course of a transaction though, to be able to produce a {@link Delta} instance, so this too is a consideration
 * for implementations.
 * <br />
 *
 * @author Manik Surtani
 * @see Delta
 * @since 4.0
 */
public interface DeltaAware {
   /**
    * Extracts changes made to implementations, in an efficient format that can easily and cheaply be serialized and
    * deserialized.  This method will only be called once for each changeset as it is assumed that any implementation's
    * internal changelog is wiped and reset after generating and submitting the delta to the caller.
    *
    * @return an instance of Delta
    */
   Delta delta();

   /**
    * Indicate that all deltas collected to date has been extracted (via a call to {@link #delta()}) and can be
    * discarded.  Often used as an optimization if the delta isn't really needed, but the cleaning and resetting of
    * internal state is desirable.
    */
   void commit();
}
