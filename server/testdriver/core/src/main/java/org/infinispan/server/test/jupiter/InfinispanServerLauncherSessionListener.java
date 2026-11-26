package org.infinispan.server.test.jupiter;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

public class InfinispanServerLauncherSessionListener implements LauncherSessionListener {
   @Override
   public void launcherSessionOpened(LauncherSession session) {
      ServerRetryLifecycle.sessionOpened(session);
   }

   @Override
   public void launcherSessionClosed(LauncherSession session) {
      ServerRetryLifecycle.sessionClosed(session);
   }
}
