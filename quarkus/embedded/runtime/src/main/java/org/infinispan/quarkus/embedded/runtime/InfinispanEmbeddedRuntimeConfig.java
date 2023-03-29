package org.infinispan.quarkus.embedded.runtime;

import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

/**
 * @author wburns
 */
@ConfigRoot(name = "infinispan-embedded", phase = ConfigPhase.RUN_TIME)
public class InfinispanEmbeddedRuntimeConfig {

    /**
     * The configured Infinispan embedded xml file which is used by the managed EmbeddedCacheManager and its Caches
     */
    @ConfigItem
    public Optional<String> xmlConfig;

    @Override
    public String toString() {
        return "InfinispanEmbeddedRuntimeConfig{" +
                "xmlConfig=" + xmlConfig +
                '}';
    }
}
