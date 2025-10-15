package org.infinispan.globalstate.impl;

import static org.infinispan.globalstate.ScopedPersistentState.GLOBAL_SCOPE;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Paths;
import java.util.Optional;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.FileSystemLock;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.GlobalStateProvider;
import org.infinispan.globalstate.ScopedPersistentState;

/**
 * GlobalStateManagerImpl. This global component manages persistent state across restarts as well as global
 * configurations. The information is stored in a Properties file. On a graceful shutdown it persists the following
 * information:
 * <p>
 * version = full version (e.g. major.minor.micro.qualifier) timestamp = timestamp using ISO-8601
 * <p>
 * as well as any additional information contributed by registered {@link GlobalStateProvider}s
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
public class GlobalStateManagerImpl implements GlobalStateManager {
   public static final String VERSION = "@version";
   public static final String TIMESTAMP = "@timestamp";
   public static final String VERSION_MAJOR = "version-major";

   @Inject
   GlobalConfiguration globalConfiguration;
   @Inject
   TimeService timeService;

   private final GlobalStateHandler handler = new  GlobalStateHandler();
   private FileSystemLock globalLock;
   private boolean persistentState;
   private ScopedPersistentState globalState;

   @Start // Must start before everything else
   public void start() {
      persistentState = globalConfiguration.globalState().enabled();
      if (persistentState) {
         handler.setRoot(globalConfiguration.globalState().persistentLocation());
         handler.setTimeService(timeService);
         acquireGlobalLock();
         loadGlobalState();
      }
   }

   @Stop // Must write global state before other global components shut down
   public void stop() {
      if (persistentState) {
         writeGlobalState();
         releaseGlobalLock();
      }
   }

   private void acquireGlobalLock() {
      globalLock = new FileSystemLock(Paths.get(globalConfiguration.globalState().persistentLocation()), GLOBAL_SCOPE);
      try {
         boolean checkAcquire = true;
         if (!globalLock.tryLock()) {
            // The node was not shutdown cleanly
            checkAcquire = switch (globalConfiguration.globalState().uncleanShutdownAction()) {
               case FAIL:
                  throw CONTAINER.globalStateLockFilePresent(globalLock);
               case PURGE:
                  deleteScopedState(GLOBAL_SCOPE);
                  globalLock.unsafeLock();
                  yield true;
               case IGNORE:
                  // Do nothing;
                  yield false;
            };
         }

         if (checkAcquire && !globalLock.isAcquired()) {
            throw CONTAINER.globalStateCannotAcquireLockFile(null, globalLock);
         }
      } catch (IOException | OverlappingFileLockException e) {
         throw CONTAINER.globalStateCannotAcquireLockFile(e, globalLock);
      }
   }

   private void releaseGlobalLock() {
      if (globalLock != null && globalLock.isAcquired())
         globalLock.unlock();
   }

   private void loadGlobalState() {
      globalState = handler.startStateHandler();
   }

   @Override
   public void writeGlobalState() {
      if (persistentState) {
         handler.writeGlobalState();
      }
   }

   @Override
   public void writeScopedState(ScopedPersistentState state) {
      if (persistentState) {
         handler.writeScopedState(state);
      }
   }

   @Override
   public void deleteScopedState(String scope) {
      if (persistentState) {
         handler.deleteScopedState(scope);
      }
   }

   @Override
   public Optional<ScopedPersistentState> readScopedState(String scope) {
      if (!persistentState)
         return Optional.empty();
      return handler.readScopedState(scope);
   }

   @Override
   public void registerStateProvider(GlobalStateProvider provider) {
      handler.registerStateProvider(provider);
      if (globalState != null) {
         provider.prepareForRestore(globalState);
      }
   }
}
