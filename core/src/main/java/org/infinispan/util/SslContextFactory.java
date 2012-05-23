/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * SslContextFactory.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslContextFactory {
   private static final Log log = LogFactory.getLog(SslContextFactory.class);

   public static SSLContext getContext(KeyManager[] keyManagers, String keyStoreFileName, char[] keyStorePassword, TrustManager[] trustManagers, String trustStoreFileName, char[] trustStorePassword) {
      try {
         if (keyManagers == null) {
            if (keyStoreFileName != null) {
               KeyStore ks = KeyStore.getInstance("JKS");
               loadKeyStore(ks, keyStoreFileName, keyStorePassword);
               KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
               kmf.init(ks, keyStorePassword);
               keyManagers = kmf.getKeyManagers();
            }
         }

         if (trustManagers == null) {
            if (trustStoreFileName != null) {
               KeyStore ks = KeyStore.getInstance("JKS");
               loadKeyStore(ks, trustStoreFileName, trustStorePassword);
               TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
               tmf.init(ks);
               trustManagers = tmf.getTrustManagers();
            }
         }
         SSLContext sslContext = SSLContext.getInstance("TLS");
         sslContext.init(keyManagers, trustManagers, null);
         return sslContext;
      } catch (Exception e) {
         throw log.sslInitializationException(e);
      }
   }

   public static SSLEngine getEngine(SSLContext sslContext, boolean useClientMode, boolean needClientAuth) {
      SSLEngine sslEngine = sslContext.createSSLEngine();
      sslEngine.setUseClientMode(useClientMode);
      sslEngine.setNeedClientAuth(needClientAuth);
      return sslEngine;
   }

   private static void loadKeyStore(KeyStore ks, String keyStoreFileName, char[] keyStorePassword) throws IOException, GeneralSecurityException {
      InputStream is = new BufferedInputStream(new FileInputStream(keyStoreFileName));
      try {
         ks.load(is, keyStorePassword);
      } finally {
         Util.close(is);
      }
   }

}
