package org.infinispan.server.hotrod;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import javax.security.sasl.SaslServerFactory;

import org.infinispan.commons.util.SaslUtils;

/**
 * Meta-factory for available {@link SaslServerFactory}
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class DefaultSaslServerFactoryFactory implements BiFunction<String, Map<String, ?>, SaslServerFactory> {
   private final SaslServerFactory factories[];

   public DefaultSaslServerFactoryFactory() {
      List<SaslServerFactory> factories = new ArrayList<>(SaslUtils.getSaslServerFactories(this.getClass().getClassLoader(), true));
      this.factories = factories.toArray(new SaslServerFactory[0]);
   }

   @Override
   public SaslServerFactory apply(String mechanism, Map<String, ?> mechProps) {
      for (SaslServerFactory factory : factories) {
         if (factory != null) {
            String[] mechs = factory.getMechanismNames(mechProps);
            for(String mech : mechs) {
               if (mech.equals(mechanism))
                  return factory;
            }
         }
      }
      return null;
   }
}
