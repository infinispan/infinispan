/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.lucene.profiling;

import java.io.IOException;

import org.apache.lucene.store.Directory;

/**
 * LuceneUserThread: base class to perform activities on the index, as searching, adding to index and
 * deleting.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public abstract class LuceneUserThread implements Runnable {

   protected final Directory directory;
   protected final SharedState state;
   
   LuceneUserThread(Directory dir, SharedState state) {
      this.directory = dir;
      this.state = state;
   }

   @Override
   public final void run() {
      try {
         state.waitForStart();
      } catch (InterruptedException e1) {
         state.errorManage(e1);
         return;
      }
      while (!state.needToQuit()) {
         try {
            testLoop();
         } catch (Exception e) {
            state.errorManage(e);
         }
      }
   }

   protected abstract void testLoop() throws IOException;

}
