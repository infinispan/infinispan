package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.logging.Logger;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

/**
 * @author Vladimir Blagojevic
 *
 */
public class CounterService implements Service<CounterConfiguration> {

    private static final Logger log = Logger.getLogger(CounterService.class.getPackage().getName());

    private final Dependencies dependencies;
    private final String counterConfigurationName;
    private final CounterConfiguration counterConfiguration;

    public interface Dependencies {
        EmbeddedCacheManager getCacheContainer();
    }

    public CounterService(CounterConfiguration configuration, String configurationName, Dependencies dependencies) {
        this.counterConfiguration = configuration;
        this.counterConfigurationName = configurationName;
        this.dependencies = dependencies;
    }

    /**
     * {@inheritDoc}
     *
     * @see org.jboss.msc.value.Value#getValue()
     */
    @Override
    public CounterConfiguration getValue() {
        return this.counterConfiguration;
    }

    @Override
    public void start(StartContext context) {
        EmbeddedCacheManager container = this.dependencies.getCacheContainer();
        CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(container);

        //define counter
        counterManager.defineCounter(counterConfigurationName, counterConfiguration);

        //but also instantiate the counter as well
        switch (counterConfiguration.type()) {
            case BOUNDED_STRONG:
            case UNBOUNDED_STRONG: {
                counterManager.getStrongCounter(counterConfigurationName);
                break;
            }
            case WEAK: {
                counterManager.getWeakCounter(counterConfigurationName);
                break;
            }
            default: {
                log.warn("Unknown counter type " + counterConfiguration.type() + " did not get instantiated");
                break;
            }
        }
    }

    @Override
    public void stop(StopContext context) {
        // intentionally empty as we don't do anything on service stop
    }
}
