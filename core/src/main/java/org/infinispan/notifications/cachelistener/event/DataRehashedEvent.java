/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.notifications.cachelistener.event;

import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * An event passed in to methods annotated with {@link DataRehashed}.
 *
 * @author Manik Surtani
 * @since 5.0
 */
public interface DataRehashedEvent extends Event {

   /**
    * @return Retrieves the list of members before rehashing started.
    */
   Collection<Address> getMembersAtStart();

   /**
    * @return Retrieves the list of members after rehashing ended.
    */
   Collection<Address> getMembersAtEnd();

   /**
    * @return Retrieves the new view id after rehashing was triggered.
    */
   long getNewViewId();
}
