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
package org.infinispan.lucene.profiling;

import java.util.HashSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SharedState is used by LuceneUserThread when used concurrently to coordinate
 * the assertions: different threads need a shared state to know what to assert
 * about the Index contents.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public class SharedState {
   
   final BlockingDeque<String> stringsInIndex = new LinkedBlockingDeque<String>();
   final BlockingDeque<String> stringsOutOfIndex = new LinkedBlockingDeque<String>();
   private final AtomicLong indexWriterActionCount = new AtomicLong();
   private final AtomicLong searchingActionCount = new AtomicLong();
   private final AtomicInteger errors = new AtomicInteger(0);
   private volatile boolean quit = false;
   private final CountDownLatch startSignal = new CountDownLatch(1);
   
   public SharedState(int initialDictionarySize) {
      HashSet<String> strings = new HashSet<String>();
      for (int i=1; i<=initialDictionarySize; i++) {
         strings.add( String.valueOf(i));
      }
      stringsOutOfIndex.addAll(strings);
   }
   
   public boolean needToQuit() {
      return quit || (errors.get()!=0);
   }
   
   public void errorManage(Exception e) {
      errors.incrementAndGet();
      e.printStackTrace();
   }

   public long incrementIndexWriterTaskCount(long delta) {
      return indexWriterActionCount.addAndGet(delta);
   }
   
   public long incrementIndexSearchesCount(long delta) {
      return searchingActionCount.addAndGet(delta);
   }

   public String getStringToAddToIndex() throws InterruptedException {
      return stringsOutOfIndex.take();
   }

   public void quit() {
      quit = true;
   }

   public void addStringWrittenToIndex(String termToAdd) {
      stringsInIndex.add(termToAdd);
   }

   public void waitForStart() throws InterruptedException {
      startSignal.await();
   }
   
   public void startWaitingThreads() {
      startSignal.countDown();
   }

}
