package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.counter.configuration.Reliability;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.CounterManagerConfiguration;
import org.jboss.msc.value.Value;

/**
 * CounterManagerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class CounterManagerConfigurationBuilder implements Value<CounterManagerConfiguration>, CounterManagerConfiguration {
    public void setNumOwners(int numOwners) {
        this.numOwners = numOwners;
    }

    public void setReliability(Reliability reliability) {
        this.reliability = reliability;
    }

    private int numOwners;
    private Reliability reliability;

    @Override
    public CounterManagerConfiguration getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

    @Override
    public int getNumOwners() {
        return numOwners;
    }

    @Override
    public Reliability getReliability() {
        return reliability;
    }
}
