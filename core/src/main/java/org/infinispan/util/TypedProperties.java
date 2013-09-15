package org.infinispan.util;

import java.util.Properties;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Deprecated //needed for cache store compatibility
public class TypedProperties extends org.infinispan.commons.util.TypedProperties {

   /** The serialVersionUID */
   private static final long serialVersionUID = 3799321248100686287L;


   public TypedProperties() {
   }

   public TypedProperties(Properties p) {
      super(p);
   }

   /**
    * Factory method that converts a JDK {@link java.util.Properties} instance to an instance of TypedProperties, if needed.
    *
    * @param p properties to convert.
    * @return A TypedProperties object.  Returns an empty TypedProperties instance if p is null.
    */
   public static TypedProperties toTypedProperties(Properties p) {
      if (p instanceof TypedProperties) return (TypedProperties) p;
      return new TypedProperties(p);
   }

}
