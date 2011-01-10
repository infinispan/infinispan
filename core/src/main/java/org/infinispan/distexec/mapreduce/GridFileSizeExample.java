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
package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.io.GridFile;
import org.infinispan.remoting.transport.Address;

public class GridFileSizeExample {

   public static void main(String arg[]) throws Exception {

      Cache <String, GridFile.Metadata> cache = null;
      MapReduceTask<String, GridFile.Metadata, Long, Long> task = new MapReduceTask<String, GridFile.Metadata, Long, Long>(cache);

      Long result = task.mappedWith(new Mapper<String, GridFile.Metadata, Long>() {

         @Override
         public Long map(String key, GridFile.Metadata value) {
            return (long) value.getLength();
         }

      }).reducedWith(new Reducer<Long, Long>() {

         @Override
         public Long reduce(Long mapResult, Long previouslyReduced) {
            return previouslyReduced == null ? mapResult : mapResult + previouslyReduced;
         }

      }).collate(new Collator<Long>(){

         private Long result = 0L;
         
         @Override
         public Long collate() {
            return result;
         }

         @Override
         public void reducedResultReceived(Address remoteNode, Long remoteResult) {
            result += remoteResult;
         }});

      System.out.println("Total filesystem size is " + result + " bytes");

   }
}
