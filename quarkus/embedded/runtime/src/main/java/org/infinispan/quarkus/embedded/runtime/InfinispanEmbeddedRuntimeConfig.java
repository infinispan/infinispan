package org.infinispan.quarkus.embedded.runtime;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;

import java.util.Optional;

/**
 * @author wburns
 */
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
@ConfigMapping(prefix = "quarkus.infinispan-embedded")
public interface InfinispanEmbeddedRuntimeConfig {

    /**
     * The configured Infinispan embedded xml file which is used by the managed EmbeddedCacheManager and its Caches
     */
    Optional<String> xmlConfig();
}
