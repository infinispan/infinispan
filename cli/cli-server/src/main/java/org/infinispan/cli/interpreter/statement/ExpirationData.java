package org.infinispan.cli.interpreter.statement;

/**
 * ExpirationData holds optional expiration information as specified in cli commands
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ExpirationData {
   final Long expires;
   final Long maxIdle;

   public ExpirationData(final Long expires, final Long maxIdle) {
      this.expires = expires;
      this.maxIdle = maxIdle;
   }
}
