package org.infinispan.cli.util;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ZeroSecurityTrustManager extends X509ExtendedTrustManager {
   @Override
   public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
   }

   @Override
   public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {

   }

   @Override
   public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {

   }

   @Override
   public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {

   }

   @Override
   public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

   }

   @Override
   public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

   }

   @Override
   public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
   }
}
