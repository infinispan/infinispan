package org.infinispan.client.openapi.impl.jdk.auth;


import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.client.openapi.configuration.AuthenticationConfiguration;

public class DigestAuthenticator extends HttpAuthenticator {
   private static final Pattern HEADER_REGEX = Pattern.compile("\\s([a-z]+)=\"?([\\p{Alnum}\\s\\t!#$%&'()*+\\-./:;<=>?@\\[\\\\\\]^_`{|}~]+)\"?");
   private static final String CREDENTIAL_CHARSET = "http.auth.credential-charset";
   private static final int QOP_UNKNOWN = -1;
   private static final int QOP_MISSING = 0;
   private static final int QOP_AUTH_INT = 1;
   private static final int QOP_AUTH = 2;
   private static final char[] HEXADECIMAL = {
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
   };
   private final Charset credentialsCharset = StandardCharsets.US_ASCII;
   private String lastNonce;
   private long nonceCount;
   private String cnonce;

   public DigestAuthenticator(HttpClient client, AuthenticationConfiguration configuration) {
      super(client, configuration);
   }

   @Override
   public <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler) {
      String header = findHeader(response, WWW_AUTH, "Digest");
      Matcher matcher = HEADER_REGEX.matcher(header);
      Map<String, String> parameters = new ConcurrentHashMap<>(8);
      while (matcher.find()) {
         parameters.put(matcher.group(1), matcher.group(2));
      }
      String nonce = parameters.get("nonce");
      if (nonce == null) {
         throw new AuthenticationException("Missing nonce in challenge header: " + header);
      }
      String realm = parameters.get("realm");
      if (realm == null) {
         return null;
      }
      boolean isStale = Boolean.parseBoolean(parameters.get("stale"));
      HttpRequest request = response.request();
      if (havePreviousDigestAuthorizationAndShouldAbort(request, isStale)) {
         return null;
      }
      parameters.put("methodname", request.method());
      parameters.put("uri", digestUri(request.uri()));
      parameters.computeIfAbsent("charset", k -> getCredentialsCharset(request));
      HttpRequest.Builder newRequest = copyRequest(request).header(WWW_AUTH_RESP, createDigestHeader(request, parameters));
      return client.sendAsync(newRequest.build(), (HttpResponse.BodyHandler<T>) bodyHandler);
   }

   private String digestUri(URI uri) {
      String query = uri.getQuery();
      if (query == null || query.isEmpty()) {
         return uri.getRawPath();
      } else {
         return uri.getRawPath() + "?" + uri.getRawQuery();
      }
   }

   public static String createCnonce() {
      final SecureRandom rnd = new SecureRandom();
      final byte[] tmp = new byte[8];
      rnd.nextBytes(tmp);
      return encode(tmp);
   }

   static String encode(final byte[] binaryData) {
      final int n = binaryData.length;
      final char[] buffer = new char[n * 2];
      for (int i = 0; i < n; i++) {
         final int low = (binaryData[i] & 0x0f);
         final int high = ((binaryData[i] & 0xf0) >> 4);
         buffer[i * 2] = HEXADECIMAL[high];
         buffer[(i * 2) + 1] = HEXADECIMAL[low];
      }
      return new String(buffer);
   }

   private boolean havePreviousDigestAuthorizationAndShouldAbort(HttpRequest request, boolean isStale) {
      final Optional<String> previousAuthorizationHeader = request.headers().firstValue(WWW_AUTH_RESP);
      if (previousAuthorizationHeader.isPresent() && previousAuthorizationHeader.get().startsWith("Digest")) {
         return !isStale;
      }
      return false;
   }

   private synchronized String createDigestHeader(HttpRequest request, Map<String, String> parameters) throws AuthenticationException {
      String uri = parameters.get("uri");
      String realm = parameters.get("realm");
      String nonce = parameters.get("nonce");
      String opaque = parameters.get("opaque");
      String method = parameters.get("methodname");
      String algorithm = parameters.getOrDefault("algorithm", "MD5");
      Set<String> qopSet = new HashSet<>(8);
      int qop = QOP_UNKNOWN;
      String qopList = parameters.get("qop");
      if (qopList != null) {
         StringTokenizer tok = new StringTokenizer(qopList, ",");
         while (tok.hasMoreTokens()) {
            final String variant = tok.nextToken().trim();
            qopSet.add(variant.toLowerCase(Locale.US));
         }
         if (request.bodyPublisher().isPresent() && qopSet.contains("auth-int")) {
            qop = QOP_AUTH_INT;
         } else if (qopSet.contains("auth")) {
            qop = QOP_AUTH;
         }
      } else {
         qop = QOP_MISSING;
      }
      if (qop == QOP_UNKNOWN) {
         throw new AuthenticationException("None of the qop methods is supported: " + qopList);
      }
      String charset = parameters.getOrDefault("charset", StandardCharsets.ISO_8859_1.name());
      String digAlg = algorithm;
      if ("MD5-sess".equalsIgnoreCase(digAlg)) {
         digAlg = "MD5";
      }
      final MessageDigest digester;
      try {
         digester = MessageDigest.getInstance(digAlg);
      } catch (Exception ex) {
         throw new AuthenticationException("Unsuppported digest algorithm: " + digAlg, ex);
      }

      String uname = configuration.username();
      String pwd = new String(configuration.password());
      if (nonce.equals(this.lastNonce)) {
         nonceCount++;
      } else {
         nonceCount = 1;
         cnonce = null;
         lastNonce = nonce;
      }
      StringBuilder sb = new StringBuilder(256);
      Formatter formatter = new Formatter(sb, Locale.US);
      formatter.format("%08x", nonceCount);
      formatter.close();
      final String nc = sb.toString();
      if (cnonce == null) {
         cnonce = createCnonce();
      }
      String s1;
      String s2;
      if ("MD5-sess".equalsIgnoreCase(algorithm)) {
         sb.setLength(0);
         sb.append(uname).append(':').append(realm).append(':').append(pwd);
         String checksum = encode(digester.digest(getBytes(sb.toString(), charset)));
         sb.setLength(0);
         sb.append(checksum).append(':').append(nonce).append(':').append(cnonce);
         s1 = sb.toString();
      } else {
         sb.setLength(0);
         sb.append(uname).append(':').append(realm).append(':').append(pwd);
         s1 = sb.toString();
      }
      final String hasha1 = encode(digester.digest(getBytes(s1, charset)));
      if (qop == QOP_AUTH) {
         s2 = method + ':' + uri;
      } else if (qop == QOP_AUTH_INT) {
         if (request.bodyPublisher().isPresent()) {
            if (qopSet.contains("auth")) {
               qop = QOP_AUTH;
               s2 = method + ':' + uri;
            } else {
               throw new AuthenticationException("Qop auth-int cannot be used with a non-repeatable entity");
            }
         } else {
            digester.reset();
            // empty content
            s2 = method + ':' + uri + ':' + encode(digester.digest());
         }
      } else {
         s2 = method + ':' + uri;
      }
      String h2 = encode(digester.digest(getBytes(s2, charset)));
      String digestValue;
      if (qop == QOP_MISSING) {
         sb.setLength(0);
         sb.append(hasha1).append(':').append(nonce).append(':').append(h2);
         digestValue = sb.toString();
      } else {
         sb.setLength(0);
         sb.append(hasha1).append(':').append(nonce).append(':').append(nc).append(':')
               .append(cnonce).append(':').append(qop == QOP_AUTH_INT ? "auth-int" : "auth")
               .append(':').append(h2);
         digestValue = sb.toString();
      }
      String digest = encode(digester.digest(getAsciiBytes(digestValue)));
      StringBuilder buffer = new StringBuilder(128);
      buffer.append("Digest username=\"");
      buffer.append(uname);
      buffer.append("\", realm=\"");
      buffer.append(realm);
      buffer.append("\", nonce=\"");
      buffer.append(nonce);
      buffer.append("\", uri=\"");
      buffer.append(uri);
      buffer.append("\", response=\"");
      buffer.append(digest);
      buffer.append("\", ");
      if (qop != QOP_MISSING) {
         buffer.append("qop=");
         buffer.append(qop == QOP_AUTH_INT ? "auth-int" : "auth");
         buffer.append(", nc=");
         buffer.append(nc);
         buffer.append(", cnonce=\"");
         buffer.append(cnonce);
         buffer.append("\", ");
      }
      buffer.append("algorithm=");
      buffer.append(algorithm);
      if (opaque != null) {
         buffer.append(", opaque=\"");
         buffer.append(opaque);
         buffer.append('"');
      }
      return buffer.toString();
   }

   private Charset getCredentialsCharset() {
      return credentialsCharset;
   }

   private String getCredentialsCharset(HttpRequest request) {
      List<String> charset = request.headers().allValues(CREDENTIAL_CHARSET);
      if (charset.isEmpty()) {
         return getCredentialsCharset().name();
      } else {
         return charset.get(0);
      }
   }

   private byte[] getBytes(final String s, final String charset) {
      try {
         return s.getBytes(charset);
      } catch (UnsupportedEncodingException e) {
         // try again with default encoding
         return s.getBytes();
      }
   }

   private static byte[] getAsciiBytes(String data) {
      if (data == null) {
         throw new IllegalArgumentException("Parameter may not be null");
      } else {
         return data.getBytes(StandardCharsets.US_ASCII);
      }
   }
}
