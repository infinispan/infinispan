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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.PriorityQueue;
import org.hibernate.search.SearchException;
import org.infinispan.util.ReflectionUtil;

/**
 * ISPNPriorityQueueFactory.
 * 
 * Factory to construct a lucene PriotityQueue (unfortunately not public classes)
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
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
      Object[] constructorArgument = new Object[]{ size };
      Class[] types = new Class[]{ int.class };
      PriorityQueue<FieldDoc> queue = buildPriorityQueueSafe(className, types, constructorArgument);
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
      Object[] constructorArgument = new Object[]{ size, false };
      Class[] types = new Class[]{ int.class, boolean.class };
      return buildPriorityQueueSafe(className, types, constructorArgument);
   }

   /**
    * @param className fully qualified name of the class to construct
    * @param types types of the constructor to use
    * @param constructorArgument arguments for the chosen constructor
    */
   private static PriorityQueue<FieldDoc> buildPriorityQueueSafe(String className,
         Class[] types, Object[] constructorArgument) {
      try {
         return buildPriorityQueue(className, types, constructorArgument);
      } catch (Exception e) {
         throw new SearchException("Could not initialize required Lucene class: " + className + 
               ". Either the Lucene version is incompatible, or security is preventing me to access it.", e);
      }
   }

   /**
    * Creates a class instance from classname, types and arguments, to workaround the
    * fact that these Lucene PriorityQueues are not public.
    * @param className
    * @param types
    * @param constructorArgument
    */
   private static PriorityQueue<FieldDoc> buildPriorityQueue(String className, Class[] types,
         java.lang.Object[] constructorArgument) throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
      Class<?> clazz = Class.forName(className);
      Constructor c = clazz.getDeclaredConstructor(types);
      c.setAccessible(true);
      Object newInstance = c.newInstance(constructorArgument);
      return (PriorityQueue<FieldDoc>) newInstance;
   }

}
