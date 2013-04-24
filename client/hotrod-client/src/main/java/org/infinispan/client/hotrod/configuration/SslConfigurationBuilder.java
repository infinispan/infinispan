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
package org.infinispan.client.hotrod.configuration;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.configuration.Builder;
import org.infinispan.client.hotrod.logging.Log;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<SslConfiguration> {
   private static final Log log = LogFactory.getLog(SslConfigurationBuilder.class);
   private boolean enabled = false;
   private String keyStoreFileName;
   private char[] keyStorePassword;
   private String trustStoreFileName;
   private char[] trustStorePassword;
   private SSLContext sslContext;

   protected SslConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Disables the SSL support
    */
   public SslConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   /**
    * Enables the SSL support
    */
   public SslConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   /**
    * Enables or disables the SSL support
    */
   public SslConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(char[])}. Alternatively specify an array of
    * {@link #keyManagers(KeyManager[])}
    */
   public SslConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      this.keyStoreFileName = keyStoreFileName;
      return this;
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)} Alternatively specify an array of
    * {@link #keyManagers(KeyManager[])}
    */
   public SslConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
   }

   public SslConfigurationBuilder sslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(char[])}. Alternatively specify an array of
    * {@link #trustManagers(TrustManager[])}
    */
   public SslConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      this.trustStoreFileName = trustStoreFileName;
      return this;
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)} Alternatively specify an array of
    * {@link #trustManagers(TrustManager[])}
    */
   public SslConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
   }

   @Override
   public void validate() {
      if (enabled) {
         if (sslContext == null) {
            if (keyStoreFileName == null) {
               throw log.noSSLKeyManagerConfiguration();
            }
            if (keyStoreFileName != null && keyStorePassword == null) {
               throw log.missingKeyStorePassword(keyStoreFileName);
            }
            if (trustStoreFileName == null) {
               throw log.noSSLTrustManagerConfiguration();
            }
            if (trustStoreFileName != null && trustStorePassword == null) {
               throw log.missingTrustStorePassword(trustStoreFileName);
            }
         } else {
            if (keyStoreFileName != null || trustStoreFileName != null) {
               throw log.xorSSLContext();
            }
         }
      }
   }

   @Override
   public SslConfiguration create() {
      return new SslConfiguration(enabled, keyStoreFileName, keyStorePassword, sslContext, trustStoreFileName, trustStorePassword);
   }

   @Override
   public SslConfigurationBuilder read(SslConfiguration template) {
      this.enabled = template.enabled();
      this.keyStoreFileName = template.keyStoreFileName();
      this.keyStorePassword = template.keyStorePassword();
      this.sslContext = template.sslContext();
      this.trustStoreFileName = template.trustStoreFileName();
      this.trustStorePassword = template.trustStorePassword();
      return this;
   }

}
