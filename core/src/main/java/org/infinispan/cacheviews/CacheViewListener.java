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

package org.infinispan.cacheviews;

/**
 * Callback interface for installing views on a cache.
 *
 * A cache-level component ({@link org.infinispan.statetransfer.StateTransferManager})
 * will implement this interface and add itself as a cache view listener in order to transfer cache state
 * to the new owners during the prepare phase.
 *
 * The view is installed in two phases, so every {@link #prepareView(CacheView,CacheView)}
 * call will be followed either by a {@link #commitView(int)} or a {@link #rollbackView(int, int)}.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
public interface CacheViewListener {
   /**
    * Called after preparing a cache view.
    */
   void prepareView(CacheView newView, CacheView oldView) throws Exception;

   /**
    * Called before committing a cache view.
    */
   void commitView(int viewId);

   /**
    * Called before rolling back a cache view installation.
    */
   void rollbackView(int newViewId, int committedViewId);

   /**
    * Called after a node left or after a merge, even if we're not preparing a new view yet we know we'll prepare one soon.
    */
   void preInstallView();

   /**
    * Called after the cache view was installed successfully.
    */
   void postInstallView(int viewId);
}
