package org.infinispan.commons.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

import org.infinispan.commons.io.FileWatcher;
import org.infinispan.commons.logging.Log;

/**
 * A {@link X509ExtendedTrustManager} which uses a @{@link FileWatcher} to check for changes.
 */
public class ReloadingX509TrustManager extends X509ExtendedTrustManager implements Closeable {
   private final AtomicReference<X509ExtendedTrustManager> manager;
   private final Path path;
   private final Function<Path, X509ExtendedTrustManager> action;
   private final FileWatcher watcher;
   private Instant lastLoaded;

   public ReloadingX509TrustManager(FileWatcher watcher, Path path, Function<Path, X509ExtendedTrustManager> action) {
      Objects.requireNonNull(watcher, "watcher must be non-null");
      Objects.requireNonNull(path, "path must be non-null");
      Objects.requireNonNull(action, "action must be non-null");
      this.manager = new AtomicReference<>();
      this.path = path;
      this.action = action;
      this.watcher = watcher;
      reload(this.path);
      watcher.watch(this.path, this::reload);
   }

   private void reload(Path path) {
      manager.set(action.apply(path));
      lastLoaded = Instant.now();
      Log.SECURITY.debugf("Loaded '%s'", path);
   }

   @Override
   public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      manager.get().checkClientTrusted(chain, authType);
   }

   @Override
   public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      manager.get().checkServerTrusted(chain, authType);
   }

   @Override
   public X509Certificate[] getAcceptedIssuers() {
      return manager.get().getAcceptedIssuers();
   }

   @Override
   public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
      manager.get().checkClientTrusted(chain, authType, socket);
   }

   @Override
   public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
      manager.get().checkServerTrusted(chain, authType, socket);
   }

   @Override
   public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
      manager.get().checkClientTrusted(chain, authType, engine);
   }

   @Override
   public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
      manager.get().checkServerTrusted(chain, authType, engine);
   }

   public Instant lastLoaded() {
      return lastLoaded;
   }

   @Override
   public void close() throws IOException {
      watcher.unwatch(path);
   }
}
