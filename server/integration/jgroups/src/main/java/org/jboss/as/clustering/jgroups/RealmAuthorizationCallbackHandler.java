package org.jboss.as.clustering.jgroups;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import org.jboss.as.core.security.RealmUser;
import org.jboss.as.core.security.SubjectUserInfo;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.AuthorizingCallbackHandler;
import org.jboss.as.domain.management.RealmConfigurationConstants;
import org.jboss.as.domain.management.SecurityRealm;

/**
 * RealmAuthorizationCallbackHandler. A {@link CallbackHandler} for JGroups which piggybacks on the
 * realm-provided {@link AuthorizingCallbackHandler}s and provides additional role validation
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class RealmAuthorizationCallbackHandler implements CallbackHandler {
    private final String mechanismName;
    private final SecurityRealm realm;
    private final String clusterRole;

    static final Collection<Principal> EMPTY_PRINCIPALS = Collections.emptySet();
    static final String SASL_OPT_REALM_PROPERTY = "com.sun.security.sasl.digest.realm";
    static final String SASL_OPT_ALT_PROTO_PROPERTY = "org.jboss.sasl.digest.alternative_protocols";
    static final String SASL_OPT_PRE_DIGESTED_PROPERTY = "org.jboss.sasl.digest.pre_digested";

    static final String DIGEST_MD5 = "DIGEST-MD5";
    static final String EXTERNAL = "EXTERNAL";
    static final String GSSAPI = "GSSAPI";
    static final String PLAIN = "PLAIN";

    public RealmAuthorizationCallbackHandler(SecurityRealm realm, String mechanismName, String clusterRole, Map<String, String> mechanismProperties) {
        this.realm = realm;
        this.mechanismName = mechanismName;
        this.clusterRole = clusterRole;
        tunePropsForMech(mechanismProperties);
    }

    private void tunePropsForMech(Map<String, String> mechanismProperties) {
        if (DIGEST_MD5.equals(mechanismName)) {
            if (!mechanismProperties.containsKey(SASL_OPT_REALM_PROPERTY)) {
                mechanismProperties.put(SASL_OPT_REALM_PROPERTY, realm.getName());
            }
            Map<String, String> mechConfig = realm.getMechanismConfig(AuthMechanism.DIGEST);
            boolean plainTextDigest = true;
            if (mechConfig.containsKey(RealmConfigurationConstants.DIGEST_PLAIN_TEXT)) {
                plainTextDigest = Boolean.parseBoolean(mechConfig.get(RealmConfigurationConstants.DIGEST_PLAIN_TEXT));
            }
            if (!plainTextDigest) {
                mechanismProperties.put(SASL_OPT_PRE_DIGESTED_PROPERTY, "true");
            }
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        AuthorizingCallbackHandler cbh = getMechCallbackHandler();
        cbh.handle(callbacks);
    }

    private AuthorizingCallbackHandler getMechCallbackHandler() {
        if (PLAIN.equals(mechanismName)) {
            return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN));
        } else if (DIGEST_MD5.equals(mechanismName)) {
            return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.DIGEST));
        } else if (GSSAPI.equals(mechanismName)) {
            return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN));
        } else if (EXTERNAL.equals(mechanismName)) {
            return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.CLIENT_CERT));
        } else {
            throw new IllegalArgumentException("Unsupported mech " + mechanismName);
        }
    }

    SubjectUserInfo validateSubjectRole(SubjectUserInfo subjectUserInfo) {
        for(Principal principal : subjectUserInfo.getPrincipals()) {
            if (clusterRole.equals(principal.getName())) {
                return subjectUserInfo;
            }
        }
        throw JGroupsMessages.MESSAGES.unauthorizedNodeJoin(subjectUserInfo.getUserName());
    }

    class DelegatingRoleAwareAuthorizingCallbackHandler implements AuthorizingCallbackHandler {
        private final AuthorizingCallbackHandler delegate;

        DelegatingRoleAwareAuthorizingCallbackHandler(AuthorizingCallbackHandler acbh) {
            this.delegate = acbh;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            AuthorizeCallback acb = findCallbackHandler(AuthorizeCallback.class, callbacks);
            if (acb != null) {
                String authenticationId = acb.getAuthenticationID();
                String authorizationId = acb.getAuthorizationID();
                acb.setAuthorized(authenticationId.equals(authorizationId));
                int realmSep = authorizationId.indexOf('@');
                RealmUser realmUser = realmSep < 0 ? new RealmUser(authorizationId) : new RealmUser(authorizationId.substring(realmSep+1), authorizationId.substring(0, realmSep));
                List<Principal> principals = new ArrayList<Principal>();
                principals.add(realmUser);
                createSubjectUserInfo(principals);
            } else {
                delegate.handle(callbacks);
            }
        }

        @Override
        public SubjectUserInfo createSubjectUserInfo(Collection<Principal> principals) throws IOException {
            // The call to the delegate will supplement the user with additional role information
            SubjectUserInfo subjectUserInfo = delegate.createSubjectUserInfo(principals);
            return validateSubjectRole(subjectUserInfo);
        }
    }

    public static <T extends Callback> T findCallbackHandler(Class<T> klass, Callback[] callbacks) {
        for(int i=0; i < callbacks.length; i++) {
            if (klass.isInstance(callbacks[i])) {
                return (T) callbacks[i];
            }
        }
        return null;
    }
}
