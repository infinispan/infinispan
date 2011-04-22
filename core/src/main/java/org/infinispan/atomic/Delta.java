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
 * Represents changes made to a {@link DeltaAware} implementation.  Implementations should be efficiently
 * {@link java.io.Externalizable} rather than just {@link java.io.Serializable}.
 *
 * @see DeltaAware
 * @author Manik Surtani
 * @since 4.0
 */
public interface Delta {
   /**
    * Merge the current Delta instance with a given {@link DeltaAware} instance, and return a coherent and complete
    * {@link DeltaAware} instance.  Implementations should be able to deal with null values passed in, or values of a
    * different type from the expected DeltaAware instance.  Usually the approach would be to ignore what is passed in,
    * create a new instance of the DeltaAware implementation that the current Delta implementation is written for, apply
    * changes and pass it back.
    *
    * @param d instance to merge with, or null if no merging is needed
    * @return a fully coherent and usable instance of DeltaAware which may or may not be the same instance passed in
    */
   DeltaAware merge(DeltaAware d);
}
