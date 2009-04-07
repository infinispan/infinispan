/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
 * This interface allows the extraction of deltas.
 * <p/>
 * Implementations would be closely coupled to a corresponding {@link Delta} implementation, since {@link
 * org.infinispan.atomic.Delta#instantiate()} would need to know how to recreate this instance of DeltaAware if needed.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @see Delta
 * @since 4.0
 */
public interface DeltaAware {
   /**
    * Extracts changes made to implementations, in an efficient format that can easily and cheaply be serialized and
    * deserialized.  This method can only be called once for each changeset as it wipes its internal changelog when
    * generating and submitting the delta to the caller.
    *
    * @return an instance of Delta
    */
   Delta delta();

   /**
    * Indicate that all deltas collected to date has been extracted (via a call to {@link #delta()}) and can be
    * discarded.
    */
   void commit();
}
