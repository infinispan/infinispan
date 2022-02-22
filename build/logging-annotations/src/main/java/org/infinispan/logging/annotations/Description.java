package org.infinispan.logging.annotations;


import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.CLASS;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Description.
 * 
 * @author Durgesh Anaokar
 * @since 13.0
 */

@Retention(CLASS)
@Target(METHOD)
public @interface Description {

   /**
    * 
    * Return the Description for the log message.
    * 
    * @return String
    */
   String value();
}
