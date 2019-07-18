package org.infinispan.client.rest.impl.okhttp;

import java.io.IOException;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Route;

public interface StatefulAuthenticator extends Authenticator {

    Request authenticateWithState(Route route, Request request) throws IOException;
}
