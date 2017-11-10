package org.infinispan.globalstate.impl;

import static org.infinispan.globalstate.ScopedPersistentState.GLOBAL_SCOPE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.infinispan.Version;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.GlobalStateProvider;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * GlobalStateManagerImpl. This global component manages persistent state across restarts as well as global configurations.
 * The information is stored in a Properties file. On a graceful shutdown it persists the following
 * information:
 *
 * version = full version (e.g. major.minor.micro.qualifier) timestamp = timestamp using ISO-8601
 *
 * as well as any additional information contributed by registered {@link GlobalStateProvider}s
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class GlobalStateManagerImpl implements GlobalStateManager {
   public static final String VERSION = "@version";
   public static final String TIMESTAMP = "@timestamp";
   public static final String VERSION_MAJOR = "version-major";
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject private GlobalConfiguration globalConfiguration;
   @Inject private TimeService timeService;

   private List<GlobalStateProvider> stateProviders = new ArrayList<>();
   private boolean persistentState;

   @Start(priority = 1) // Must start before everything else
   public void start() {
      persistentState = globalConfiguration.globalState().enabled();
      if (persistentState) {
         loadGlobalState();
      }
   }

   @Stop(priority = 1) // Must write global state before other global components shut down
   public void stop() {
      if (persistentState) {
         writeGlobalState();
      }
   }

   private void loadGlobalState() {
      File stateFile = getStateFile(GLOBAL_SCOPE);
      Optional<ScopedPersistentState> globalState = readScopedState(GLOBAL_SCOPE);
      if (globalState.isPresent()) {
         ScopedPersistentState state = globalState.get();
         // We proceed only if we can write to the file
         if (!stateFile.canWrite()) {
            throw log.nonWritableStateFile(stateFile);
         }
         // Validate the state before proceeding
         log.globalStateLoad(state.getProperty(VERSION), state.getProperty(TIMESTAMP));

         stateProviders.forEach(provider -> provider.prepareForRestore(state));
      } else {
         // Clean slate. Try to create an empty state file before proceeding
         try {
            stateFile.getParentFile().mkdirs();
            stateFile.createNewFile();
         } catch (IOException e) {
            throw log.nonWritableStateFile(stateFile);
         }
      }
   }

   @Override
   public void writeGlobalState() {
      if (persistentState) {
         ScopedPersistentState state = new ScopedPersistentStateImpl(GLOBAL_SCOPE);
         state.setProperty(VERSION, Version.getVersion());
         state.setProperty(VERSION_MAJOR, Version.getMajor());
         state.setProperty(TIMESTAMP, timeService.instant().toString());
         // ask any state providers to contribute to the global state
         stateProviders.forEach(provider -> provider.prepareForPersist(state));
         writeScopedState(state);
         log.globalStateWrite(state.getProperty(VERSION), state.getProperty(TIMESTAMP));
      }
   }

   @Override
   public void writeScopedState(ScopedPersistentState state) {
      if (persistentState) {
         File stateFile = getStateFile(state.getScope());
         try (PrintWriter w = new PrintWriter(stateFile)) {
            state.forEach((key, value) -> {
               w.printf("%s=%s%n", Util.unicodeEscapeString(key), Util.unicodeEscapeString(value));
            });
         } catch (IOException e) {
            throw log.failedWritingGlobalState(e, stateFile);
         }
      }
   }

   @Override
   public Optional<ScopedPersistentState> readScopedState(String scope) {
      if (!persistentState)
         return Optional.empty();
      File stateFile = getStateFile(scope);
      if (!stateFile.exists())
         return Optional.empty();
      try (BufferedReader r = new BufferedReader(new FileReader(stateFile))) {
         ScopedPersistentState state = new ScopedPersistentStateImpl(scope);
         for (String line = r.readLine(); line != null; line = r.readLine()) {
            if (!line.startsWith("#")) { // Skip comment lines
               int eq = line.indexOf('=');
               while (eq > 0 && line.charAt(eq-1) == '\\') {
                  eq = line.indexOf('=', eq + 1);
               }
               if (eq > 0) {
                  state.setProperty(Util.unicodeUnescapeString(line.substring(0, eq).trim()),
                        Util.unicodeUnescapeString(line.substring(eq + 1).trim()));
               }
            }
         }
         return Optional.of(state);
      } catch (IOException e) {
         throw log.failedReadingPersistentState(e, stateFile);
      }
   }

   private File getStateFile(String scope) {
      return new File(globalConfiguration.globalState().persistentLocation(), scope + ".state");
   }

   @Override
   public void registerStateProvider(GlobalStateProvider provider) {
      this.stateProviders.add(provider);
   }
}
