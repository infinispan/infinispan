package org.infinispan.server.test.junit5;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

@Target({ ElementType.FIELD, ElementType.PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(CustomInfinispanExtension.class)
public @interface InfinispanServer {
   /**
    * Defines the default suite to use if one is not defined. InfinispanServer will pick up any extension defined by
    * a suite, but if the test is ran isolated it will use the Suite defined here along with its extensions.
    * <p>
    * Can be overridden by using the system property: `org.infinispan.test.server.junit5.extension` with a value
    * that maps to a class that extends {@link InfinispanSuite}.
    * @return suite class that has an extension
    */
   Class<? extends InfinispanSuite> value();
}
