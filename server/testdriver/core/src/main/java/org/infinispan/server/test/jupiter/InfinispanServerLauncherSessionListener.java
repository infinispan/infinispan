package org.infinispan.server.test.jupiter;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

public class InfinispanServerLauncherSessionListener implements LauncherSessionListener {
   @Override
   public void launcherSessionClosed(LauncherSession session) {
      ServerRetryLifecycle.completeDeferredCleanups();
   }
}
