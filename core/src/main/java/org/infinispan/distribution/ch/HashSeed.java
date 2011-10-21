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

package org.infinispan.distribution.ch;

import org.infinispan.remoting.transport.Address;

import java.util.Set;

/**
 * A hash allows seed address to for consistent hash calculation to be
 * configured. This is particularly useful when Infinispan is accessed remotely
 * and clients are to calculate hash ids. Since clients are only aware of
 * server endpoints, implementations of {@link HashSeed} can seed based on this
 * information instead of the traditional cluster address.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface HashSeed {

   /**
    * Given a cluster address, provide an {@link Address} implementation to
    * use as node consistent hash seed.
    */
   Address getHashSeed(Address clusterMember);

}
