package org.infinispan.server.test.core.rollingupgrade;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

public record RollingUpgradeConfiguration(int nodeCount, String fromVersion, String toVersion, boolean xSite,
                                          String jgroupsProtocol, int serverCheckTimeSecs, boolean useSharedDataMount,
                                          BiConsumer<Throwable, RollingUpgradeHandler> exceptionHandler,
                                          Consumer<String> logConsumer,
                                          Consumer<RollingUpgradeHandler> initialHandler,
                                          Predicate<RollingUpgradeHandler> isValidServerState) {
}
