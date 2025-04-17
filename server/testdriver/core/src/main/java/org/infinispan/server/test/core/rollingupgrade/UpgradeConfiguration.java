package org.infinispan.server.test.core.rollingupgrade;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

public record UpgradeConfiguration(int nodeCount, String fromVersion, String toVersion, boolean xSite,
                                   String jgroupsProtocol, int serverCheckTimeSecs, boolean useSharedDataMount,
                                   BiConsumer<Throwable, UpgradeHandler> exceptionHandler,
                                   Consumer<String> logConsumer,
                                   Function<RemoteCacheManager, RemoteCache<String, String>> initialHandler) {
}
