package org.infinispan.external;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @since 14.0
 **/
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.SOURCE)
public @interface JGroupsProtocolComponent {
   String value();
}
