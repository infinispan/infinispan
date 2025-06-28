package org.infinispan.server.test.core.rollingupgrade;

import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

public record RollingUpgradeConfiguration(int nodeCount, String fromVersion, String toVersion, boolean xSite,
                                          String jgroupsProtocol, int serverCheckTimeSecs, boolean useSharedDataMount,
                                          String serverConfigurationFile, boolean defaultServerConfigurationFile,
                                          Properties properties, JavaArchive[] customArtifacts, String[] mavenArtifacts,
                                          BiConsumer<Throwable, RollingUpgradeHandler> exceptionHandler,
                                          Consumer<RollingUpgradeHandler> initialHandler,
                                          Predicate<RollingUpgradeHandler> isValidServerState,
                                          Function<ConfigurationBuilder, ConfigurationBuilder> configurationHandler) {
}
