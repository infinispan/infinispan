/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.metadata;

/**
 * Utility method for Metadata classes.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class Metadatas {

   private Metadatas() {
   }

   /**
    * Applies version in source metadata to target metadata, if no version
    * in target metadata. This method can be useful in scenarios where source
    * version information must be kept around, i.e. write skew, or when
    * reading metadata from cache store.
    *
    * @param source Metadata object which is source, whose version might be
    *               is of interest for the target metadata
    * @param target Metadata object on which version might be applied
    * @return either, the target Metadata instance as it was when it was
    * called, or a brand new target Metadata instance with version from source
    * metadata applied.
    */
   public static Metadata applyVersion(Metadata source, Metadata target) {
      if (target.version() == null && source.version() != null)
         return target.builder().version(source.version()).build();

      return target;
   }

}
