package org.infinispan.server.core;

import javax.security.auth.Subject

/**
 * Server Constant values
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
trait ServerConstants {
   val EXPIRATION_NONE = -1
   val EXPIRATION_DEFAULT = -2

   val ANONYMOUS = new Subject
}
