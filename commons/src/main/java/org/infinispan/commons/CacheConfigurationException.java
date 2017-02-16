package org.infinispan.commons;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * An exception that represents an error in the configuration.  This could be a parsing error or a logical error
 * involving clashing configuration options or missing mandatory configuration elements.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 *
 * @since 4.0
 */
public class CacheConfigurationException extends CacheException {

   /** The serialVersionUID */
   private static final long serialVersionUID = -7103679310393205388L;
   private static final Log log = LogFactory.getLog(CacheConfigurationException.class);

   public CacheConfigurationException(Exception e) {
      super(e);
   }

   public CacheConfigurationException(String string) {
      super(string);
   }

   @Deprecated
   public CacheConfigurationException(String string, String erroneousAttribute) {
      super(string);
   }

   public CacheConfigurationException(String string, Throwable throwable) {
      super(string, throwable);
   }

   @Deprecated
   public List<String> getErroneousAttributes() {
      return Collections.emptyList();
   }

   @Deprecated
   public void addErroneousAttribute(String s) {
      // Do nothing
   }

   public static Optional<RuntimeException> fromMultipleRuntimeExceptions(List<RuntimeException> exceptions) {
      switch (exceptions.size()) {
         case 0:
            return Optional.empty();
         case 1: {
            RuntimeException e = exceptions.get(0);
            return e instanceof CacheConfigurationException ? Optional.of(e) : Optional.of(new CacheConfigurationException(e));
         }
         default: {
            CacheConfigurationException exception = log.multipleConfigurationValidationErrors();
            exceptions.forEach(e -> exception.addSuppressed(e));
            return Optional.of(exception);
         }
      }
   }
}
