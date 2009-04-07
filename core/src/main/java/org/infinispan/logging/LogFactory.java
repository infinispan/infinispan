package org.infinispan.logging;

/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class LogFactory {

   private static final boolean IS_LOG4J_AVAILABLE;

   static {
      boolean available;
      try {
         Class.forName("org.apache.log4j.Logger");
         available = true;
      }
      catch (ClassNotFoundException cnfe) {
         available = false;
      }
      IS_LOG4J_AVAILABLE = available;
   }

   public static Log getLog(Class clazz) {
      return getLog(clazz.getName());
   }

   public static Log getLog(String category) {
      if (IS_LOG4J_AVAILABLE) {
         return new Log4JLogImpl(category);
      } else {
         return new JDKLogImpl(category);
      }
   }
}
