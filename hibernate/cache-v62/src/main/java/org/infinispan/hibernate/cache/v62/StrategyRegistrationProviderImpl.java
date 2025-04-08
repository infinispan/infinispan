package org.infinispan.hibernate.cache.v62;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.boot.registry.selector.SimpleStrategyRegistrationImpl;
import org.hibernate.boot.registry.selector.StrategyRegistration;
import org.hibernate.boot.registry.selector.StrategyRegistrationProvider;
import org.hibernate.cache.spi.RegionFactory;
import org.kohsuke.MetaInfServices;

/**
 * Makes the contained region factory implementations available to the Hibernate
 * {@link org.hibernate.boot.registry.selector.spi.StrategySelector} service.
 *
 * @author Steve Ebersole
 */
@MetaInfServices(StrategyRegistrationProvider.class)
public class StrategyRegistrationProviderImpl implements StrategyRegistrationProvider {
   @Override
   public Iterable<StrategyRegistration> getStrategyRegistrations() {
      final List<StrategyRegistration> strategyRegistrations = new ArrayList<StrategyRegistration>();

      strategyRegistrations.add(
            new SimpleStrategyRegistrationImpl<>(
                  RegionFactory.class,
                  InfinispanRegionFactory.class,
                  "infinispan",
                  "infinispan-jndi",
                  InfinispanRegionFactory.class.getName(),
                  InfinispanRegionFactory.class.getSimpleName()
            )
      );
      return strategyRegistrations;
   }
}
