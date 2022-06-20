package org.infinispan.client.rest.impl.okhttp.auth;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

import okhttp3.Authenticator;
import okhttp3.Headers;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Route;
import okhttp3.internal.http.RequestLine;

public class DigestAuthenticator extends AbstractAuthenticator implements CachingAuthenticator {
   private static final Pattern HEADER_REGEX = Pattern.compile("\\s([a-z]+)=\"?([\\p{Alnum}\\s\\t!#$%&'()*+\\-./:;<=>?@\\[\\\\\\]^_`{|}~]+)\"?");

   private static final String CREDENTIAL_CHARSET = "http.auth.credential-charset";
   private static final int QOP_UNKNOWN = -1;
   private static final int QOP_MISSING = 0;
   private static final int QOP_AUTH_INT = 1;
   private static final int QOP_AUTH = 2;

   private static final char[] HEXADECIMAL = {
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
         'e', 'f'
   };

   private final AtomicReference<Map<String, String>> parametersRef = new AtomicReference<>();
   private final Charset credentialsCharset = StandardCharsets.US_ASCII;
   private final AuthenticationConfiguration configuration;
   private String lastNonce;
   private long nounceCount;
   private String cnonce;

   public DigestAuthenticator(AuthenticationConfiguration configuration) {
      this.configuration = configuration;
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

   @Override
   public synchronized Request authenticate(Route route, Response response) throws IOException {
      String header = findHeader(response.headers(), WWW_AUTH, "Digest");
      Matcher matcher = HEADER_REGEX.matcher(header);
      Map<String, String> parameters = new ConcurrentHashMap<>(8);
      while (matcher.find()) {
         parameters.put(matcher.group(1), matcher.group(2));
      }
      copyHeaderMap(response.headers(), parameters);
      parametersRef.set(Collections.unmodifiableMap(parameters));

      if (parameters.get("nonce") == null) {
         throw new IllegalArgumentException("missing nonce in challenge header: " + header);
      }

      return authenticateWithState(route, response.request(), parameters);
   }

   @Override
   public Request authenticateWithState(Route route, Request request) throws IOException {
      Map<String, String> ref = parametersRef.get();
      Map<String, String> parameters = ref == null
            ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(ref);
      return authenticateWithState(route, request, parameters);
   }

   private Request authenticateWithState(Route route, Request request, Map<String, String> parameters) throws IOException {
      final String realm = parameters.get("realm");
      if (realm == null) {
         return null;
      }
      final String nonce = parameters.get("nonce");
      if (nonce == null) {
         throw new IllegalArgumentException("missing nonce in challenge");
      }
      String stale = parameters.get("stale");
      boolean isStale = "true".equalsIgnoreCase(stale);

      if (havePreviousDigestAuthorizationAndShouldAbort(request, nonce, isStale)) {
         return null;
      }

      // Add method name and request-URI to the parameter map
      if (route == null || !route.requiresTunnel()) {
         final String method = request.method();
         final String uri = RequestLine.requestPath(request.url());
         parameters.put("methodname", method);
         parameters.put("uri", uri);
      } else {
         final String method = "CONNECT";
         final String uri = request.url().host() + ':' + request.url().port();
         parameters.put("methodname", method);
         parameters.put("uri", uri);
      }

      final String charset = parameters.get("charset");
      if (charset == null) {
         String credentialsCharset = getCredentialsCharset(request);
         parameters.put("charset", credentialsCharset);
      }
      return request.newBuilder()
            .header(WWW_AUTH_RESP, createDigestHeader(request, parameters))
            .tag(Authenticator.class, this)
            .build();
   }

   private boolean havePreviousDigestAuthorizationAndShouldAbort(Request request, String nonce, boolean isStale) {
      final String headerKey;
      headerKey = WWW_AUTH_RESP;
      final String previousAuthorizationHeader = request.header(headerKey);
      if (previousAuthorizationHeader != null && previousAuthorizationHeader.startsWith("Digest")) {
         return !isStale;
      }
      return false;
   }

   private void copyHeaderMap(Headers headers, Map<String, String> dest) {
      for (int i = 0; i < headers.size(); i++) {
         dest.put(headers.name(i), headers.value(i));
      }
   }

   private synchronized String createDigestHeader(
         final Request request,
         final Map<String, String> parameters) throws AuthenticationException {
      final String uri = parameters.get("uri");
      final String realm = parameters.get("realm");
      final String nonce = parameters.get("nonce");
      final String opaque = parameters.get("opaque");
      final String method = parameters.get("methodname");
      String algorithm = parameters.get("algorithm");
      // If an algorithm is not specified, default to MD5.
      if (algorithm == null) {
         algorithm = "MD5";
      }

      final Set<String> qopset = new HashSet<>(8);
      int qop = QOP_UNKNOWN;
      final String qoplist = parameters.get("qop");
      if (qoplist != null) {
         final StringTokenizer tok = new StringTokenizer(qoplist, ",");
         while (tok.hasMoreTokens()) {
            final String variant = tok.nextToken().trim();
            qopset.add(variant.toLowerCase(Locale.US));
         }
         if (request.body() != null && qopset.contains("auth-int")) {
            qop = QOP_AUTH_INT;
         } else if (qopset.contains("auth")) {
            qop = QOP_AUTH;
         }
      } else {
         qop = QOP_MISSING;
      }

      if (qop == QOP_UNKNOWN) {
         throw new AuthenticationException("None of the qop methods is supported: " + qoplist);
      }

      String charset = parameters.get("charset");
      if (charset == null) {
         charset = StandardCharsets.ISO_8859_1.name();
      }

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

      final String uname = configuration.username();
      final String pwd = new String(configuration.password());

      if (nonce.equals(this.lastNonce)) {
         nounceCount++;
      } else {
         nounceCount = 1;
         cnonce = null;
         lastNonce = nonce;
      }
      final StringBuilder sb = new StringBuilder(256);
      final Formatter formatter = new Formatter(sb, Locale.US);
      formatter.format("%08x", nounceCount);
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
         final String checksum = encode(digester.digest(getBytes(sb.toString(), charset)));
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
         RequestBody entity = request.body();
         if (entity != null) {
            if (qopset.contains("auth")) {
               qop = QOP_AUTH;
               s2 = method + ':' + uri;
            } else {
               throw new AuthenticationException("Qop auth-int cannot be used with " +
                     "a non-repeatable entity");
            }
         } else {
            digester.reset();
            // empty content
            s2 = method + ':' + uri + ':' + encode(digester.digest());
         }
      } else {
         s2 = method + ':' + uri;
      }

      final String h2 = encode(digester.digest(getBytes(s2, charset)));

      final String digestValue;
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

      final String digest = encode(digester.digest(getAsciiBytes(digestValue)));

      final StringBuilder buffer = new StringBuilder(128);

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


   /**
    * Returns the charset used for the credentials.
    *
    * @return the credentials charset
    */
   public Charset getCredentialsCharset() {
      return credentialsCharset;
   }

   String getCredentialsCharset(final Request request) {
      String charset = request.header(CREDENTIAL_CHARSET);
      if (charset == null) {
         charset = getCredentialsCharset().name();
      }
      return charset;
   }

   private byte[] getBytes(final String s, final String charset) {
      try {
         return s.getBytes(charset);
      } catch (UnsupportedEncodingException e) {
         // try again with default encoding
         return s.getBytes();
      }
   }

   public static byte[] getAsciiBytes(String data) {
      if (data == null) {
         throw new IllegalArgumentException("Parameter may not be null");
      } else {
         return data.getBytes(StandardCharsets.US_ASCII);
      }
   }


}
