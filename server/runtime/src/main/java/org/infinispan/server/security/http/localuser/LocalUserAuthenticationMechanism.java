/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2015 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.infinispan.server.security.http.localuser;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.wildfly.common.Assert.checkNotNullParam;
import static org.wildfly.security.http.HttpConstants.AUTHORIZATION;
import static org.wildfly.security.http.HttpConstants.UNAUTHORIZED;
import static org.wildfly.security.http.HttpConstants.WWW_AUTHENTICATE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.wildfly.common.iteration.ByteIterator;
import org.wildfly.common.iteration.CodePointIterator;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerRequest;
import org.wildfly.security.http.HttpServerResponse;
import org.wildfly.security.mechanism.http.UsernamePasswordAuthenticationMechanism;

/**
 * Implementation of the HTTP Local authentication mechanism
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
final class LocalUserAuthenticationMechanism extends UsernamePasswordAuthenticationMechanism {
   public static final String LOCALUSER_NAME = "LOCALUSER";

   static final String SILENT = "silent";

   public static final String LOCAL_USER_USE_SECURE_RANDOM = "wildfly.http.local-user.use-secure-random";
   public static final String LOCAL_USER_CHALLENGE_PATH = "wildfly.http.local-user.challenge-path";
   public static final String DEFAULT_USER = "wildfly.http.local-user.default-user";
   private static final String CHALLENGE_PREFIX = "Localuser ";
   private static final int PREFIX_LENGTH = CHALLENGE_PREFIX.length();
   private static final byte UTF8NUL = 0x00;

   private final boolean useSecureRandom;

   /**
    * If silent is true then this mechanism will only take effect if there is an Authorization header.
    * <p>
    * This allows you to combine basic auth with form auth, so human users will use form based auth, but allows
    * programmatic clients to login using basic auth.
    */
   private final boolean silent;
   private final File basePath;

   /**
    * Construct a new instance of {@code BasicAuthenticationMechanism}.
    *
    * @param callbackHandler the {@link CallbackHandler} to use to verify the supplied credentials and to notify to
    *                        establish the current identity.
    */
   LocalUserAuthenticationMechanism(final CallbackHandler callbackHandler, final boolean silent) {
      super(checkNotNullParam("callbackHandler", callbackHandler));
      this.silent = silent;
      this.useSecureRandom = true;
      this.basePath = new File(System.getProperty("java.io.tmpdir"));
   }

   /**
    * @see org.wildfly.security.http.HttpServerAuthenticationMechanism#getMechanismName()
    */
   @Override
   public String getMechanismName() {
      return LOCALUSER_NAME;
   }

   /**
    * @throws HttpAuthenticationException
    * @see org.wildfly.security.http.HttpServerAuthenticationMechanism#evaluateRequest(HttpServerRequest)
    */
   @Override
   public void evaluateRequest(final HttpServerRequest request) throws HttpAuthenticationException {
      List<String> authorizationValues = request.getRequestHeaderValues(AUTHORIZATION);
      if (authorizationValues != null) {
         for (String current : authorizationValues) {
            if (current.startsWith(CHALLENGE_PREFIX)) {
               byte[] decodedValue = ByteIterator.ofBytes(current.substring(PREFIX_LENGTH).getBytes(UTF_8)).asUtf8String().base64Decode().drain();

               if (decodedValue.length == 0) {
                  // Initial response, create the file
                  final Random random = getRandom();
                  File challengeFile;
                  try {
                     challengeFile = File.createTempFile("local", ".challenge", basePath);
                  } catch (IOException e) {
                     throw new RuntimeException(e);
                  }
                  final FileOutputStream fos;
                  try {
                     fos = new FileOutputStream(challengeFile);
                  } catch (FileNotFoundException e) {
                     throw new RuntimeException(e);
                  }
                  boolean ok = false;
                  final byte[] bytes;
                  try {
                     bytes = new byte[8];
                     random.nextBytes(bytes);
                     try {
                        fos.write(bytes);
                        fos.close();
                        ok = true;
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                     }
                  } finally {
                     if (!ok) {
                        deleteChallenge(null);
                     }
                     try {
                        fos.close();
                     } catch (Throwable ignored) {
                     }
                  }
                  //challengeBytes = bytes;
                  final String path = challengeFile.getAbsolutePath();
                  final byte[] response = CodePointIterator.ofString(path).asUtf8(true).drain();
               } else {
                  try {
                     request.authenticationComplete();
                     if (true) {
                        succeed();
                        request.authenticationComplete();
                        return;
                     } else {
                        fail();
                        request.authenticationFailed("auth failed", response -> prepareResponse(request, response));
                        return;
                     }

                  } catch (IOException | UnsupportedCallbackException e) {
                     throw new HttpAuthenticationException(e);
                  }
               }
            }
         }
      }
   }

   private void prepareResponse(final HttpServerRequest request, HttpServerResponse response) {
      if (silent) {
         //if silent we only send a challenge if the request contained auth headers
         //otherwise we assume another method will send the challenge
         String authHeader = request.getFirstRequestHeaderValue(AUTHORIZATION);
         if (authHeader == null) {
            return;     //CHALLENGE NOT SENT
         }
      }
      StringBuilder sb = new StringBuilder(CHALLENGE_PREFIX);
      response.addResponseHeader(WWW_AUTHENTICATE, sb.toString());
      response.setStatusCode(UNAUTHORIZED);
   }

   private void deleteChallenge(File challengeFile) {
      if (challengeFile != null) {
         challengeFile.delete();
         challengeFile = null;
      }
   }

   private Random getRandom() {
      if (useSecureRandom) {
         return new SecureRandom();
      } else {
         return new Random();
      }
   }
}
