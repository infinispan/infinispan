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

package org.infinispan.lucene.impl;


/**
 * Responsible for reading from <code>InfinispanDirectory</code>.
 * This compile unit is separate than InfinispanIndexInput as it needs
 * to be compiled in the context of Lucene 3.
 * 
 * @since 5.2
 * @author Sanne Grinovero
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.IndexInput
 */
public final class InfinispanIndexInputV3 extends InfinispanIndexInput {

   public InfinispanIndexInputV3(IndexInputContext openInput) {
      super(openInput);
   }

   @Override
   public InfinispanIndexInputV3 clone() {
      InfinispanIndexInputV3 clone = (InfinispanIndexInputV3)super.clone();
      // reference counting doesn't work properly: need to use isClone
      // as in other Directory implementations. Apparently not all clones
      // are cleaned up, but the original is (especially .tis files)
      clone.isClone = true; 
      return clone;
    }

}
