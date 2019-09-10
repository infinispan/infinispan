package org.infinispan.client.rest.impl.okhttp.auth;

import java.io.IOException;
import java.util.Map;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

public class CachingAuthenticatorWrapper implements Authenticator {
    private final Authenticator innerAuthenticator;
    private final Map<String, CachingAuthenticator> authCache;

    public CachingAuthenticatorWrapper(Authenticator innerAuthenticator, Map<String, CachingAuthenticator> authCache) {
        this.innerAuthenticator = innerAuthenticator;
        this.authCache = authCache;
    }

    @Override
    public Request authenticate(Route route, Response response) throws IOException {
        Request authenticated = innerAuthenticator.authenticate(route, response);
        if (authenticated != null) {
            String authorizationValue = authenticated.header("Authorization");
            if (authorizationValue != null && innerAuthenticator instanceof CachingAuthenticator) {
                final String key = CachingAuthenticator.getCachingKey(authenticated);
                authCache.put(key, (CachingAuthenticator) authenticated.tag(Authenticator.class));
            }
        }
        return authenticated;
    }
}
