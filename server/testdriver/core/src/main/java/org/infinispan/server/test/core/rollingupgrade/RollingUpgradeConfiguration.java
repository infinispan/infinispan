package org.infinispan.server.test.core.rollingupgrade;

import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.test.core.InfinispanServerListener;
import org.jboss.shrinkwrap.api.Archive;

public record RollingUpgradeConfiguration(int nodeCount, RollingUpgradeVersion fromVersion, RollingUpgradeVersion toVersion, String name,
                                          String jgroupsProtocol, int serverCheckTimeSecs, boolean useSharedDataMount,
                                          String serverConfigurationFile, boolean defaultServerConfigurationFile,
                                          Properties properties, Archive<?>[] customArtifacts, String[] mavenArtifacts,
                                          List<InfinispanServerListener> listeners,
                                          BiConsumer<Throwable, RollingUpgradeHandler> exceptionHandler,
                                          Consumer<RollingUpgradeHandler> initialHandler,
                                          Predicate<RollingUpgradeHandler> isValidServerState,
                                          Function<ConfigurationBuilder, ConfigurationBuilder> configurationHandler) {
}
