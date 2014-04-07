package org.infinispan.query.clustered;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.PriorityQueue;
import org.hibernate.search.SearchException;
import org.infinispan.query.indexmanager.InfinispanCommandsBackend;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private static final Log log = LogFactory.getLog(ISPNPriorityQueueFactory.class, Log.class);

   private ISPNPriorityQueueFactory() {
   }

   /**
    * Creates a org.apache.lucene.search.FieldDocSortedHitQueue instance and set the size and sort
    * fields
    *
    * @param size
    * @param sort
    * @return a PriorityQueue<ScoreDoc> instance
    * @throws IOException 
    */
   public static PriorityQueue<ScoreDoc> getFieldDocSortedHitQueue(int size, SortField[] sort) {
      //The Lucene API is hiding the generic type and forcing a different type compatibility, so we need
      //to avoid generics usage.
      FieldValueHitQueue queue;
      try {
         queue = FieldValueHitQueue.create(sort, size);
      } catch (IOException e) {
         throw log.unexpectedIOException(e);
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
   public static PriorityQueue<ScoreDoc> getHitQueue(int size) {
      String className = "org.apache.lucene.search.HitQueue";
      Object[] constructorArgument = new Object[]{ size, false };
      Class<?>[] types = new Class[]{ int.class, boolean.class };
      return buildPriorityQueueSafe(className, types, constructorArgument);
   }

   /**
    * @param className fully qualified name of the class to construct
    * @param types types of the constructor to use
    * @param constructorArgument arguments for the chosen constructor
    */
   private static PriorityQueue<ScoreDoc> buildPriorityQueueSafe(String className,
         Class<?>[] types, Object[] constructorArgument) {
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
   private static PriorityQueue<ScoreDoc> buildPriorityQueue(String className, Class<?>[] types,
         java.lang.Object[] constructorArgument) throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
      Class<?> clazz = Class.forName(className);
      Constructor<?> c = clazz.getDeclaredConstructor(types);
      c.setAccessible(true);
      Object newInstance = c.newInstance(constructorArgument);
      return (PriorityQueue<ScoreDoc>) newInstance;
   }

}
