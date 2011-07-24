/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.query.clustered;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.PriorityQueue;
import org.infinispan.util.ReflectionUtil;

/**
 * ISPNPriorityQueueFactory.
 * 
 * Factory to construct a lucene PriotityQueue (unfortunately not public classes)
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
class ISPNPriorityQueueFactory {

   private ISPNPriorityQueueFactory() {

   }

   /**
    * Creates a org.apache.lucene.search.FieldDocSortedHitQueue instance and set the size and sort
    * fields
    * 
    * @param size
    * @param sort
    * @return a PriorityQueue<FieldDoc> instance
    */
   public static PriorityQueue<FieldDoc> getFieldDocSortedHitQueue(int size, SortField[] sort) {
      String className = "org.apache.lucene.search.FieldDocSortedHitQueue";

      PriorityQueue<FieldDoc> queue = createPriorityHitQueue(className, size);
      Method[] methods = queue.getClass().getDeclaredMethods();
      
      for(Method method : methods){
    	  if(method.getName().equals("setFields")){
    		  Object[] parameters = new Object[1];
    		  parameters[0] = sort;
    		  ReflectionUtil.invokeAccessibly(queue, method, parameters);
    	  }
      }
      
      return queue;
   }

   /**
    * Creates a org.apache.lucene.search.HitQueue instance and set the size
    * 
    * @param size
    * @param sort
    * @return a PriorityQueue<FieldDoc> instance
    */
   public static PriorityQueue<FieldDoc> getHitQueue(int size) {
      String className = "org.apache.lucene.search.HitQueue";

      // className, size of queue, pre populate with sentinels
      return createPriorityHitQueue(className, size, false);
   }

   private static PriorityQueue<FieldDoc> createPriorityHitQueue(String className, Object ... params) {
      try {
         Class clazz = Class.forName(className);

         Constructor c = clazz.getDeclaredConstructors()[0];
         AccessibleObject ao = c;
         ao.setAccessible(true);

         Object ob = null;
         if(params.length == 1)
            ob = c.newInstance(params[0]);
         else if(params.length == 2)
        	 ob = c.newInstance(params[0], params[1]);
         else
        	 throw new IllegalArgumentException("Wrong number of arguments...");

         return (PriorityQueue<FieldDoc>) ob;
      } catch (IllegalArgumentException e) {
         throw new RuntimeException("Could not create PriotityQueue instance...", e);
      } catch (InstantiationException e) {
         throw new RuntimeException("Could not create PriotityQueue instance...", e);
      } catch (IllegalAccessException e) {
         throw new RuntimeException("Could not create PriotityQueue instance...", e);
      } catch (InvocationTargetException e) {
         throw new RuntimeException("Could not create PriotityQueue instance...", e);
      } catch (ClassNotFoundException e) {
         throw new RuntimeException("Could not create PriotityQueue instance...", e);
      }
   }

}
