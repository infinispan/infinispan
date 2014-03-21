package org.infinispan.server.core.security;

import java.security.Principal;
import java.util.Collection;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

/**
 * AuthorizingCallbackHandler. A {@link CallbackHandler} which allows retrieving the {@link Subject} which has been authorized wrapped in a {@link SubjectUserInfo}
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface AuthorizingCallbackHandler extends CallbackHandler {

    public SubjectUserInfo getSubjectUserInfo(Collection<Principal> principals);

}
