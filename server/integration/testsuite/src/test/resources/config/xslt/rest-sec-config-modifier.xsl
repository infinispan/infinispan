<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="xml" indent="yes"/>

    <xsl:variable name="nsS">urn:jboss:domain:security:</xsl:variable>
    <xsl:variable name="nsE">urn:infinispan:server:endpoint:</xsl:variable>
    <xsl:variable name="nsW">urn:jboss:domain:web:</xsl:variable>

    <xsl:param name="security.realm" select="'ApplicationRealm'"/>
    <xsl:param name="auth.method" select="'BASIC'"/>
    <xsl:param name="cache.container" select="'${connector.cache.container}'"/>
    <xsl:param name="modifyCertSecRealm" select="false"/>
    <xsl:param name="modifyDigestSecRealm" select="false"/>

    <!-- New rest-connector definition -->
    <xsl:variable name="newRESTEndpointDefinition">
        <xsl:choose>
            <xsl:when test="$auth.method != 'CLIENT_CERT'">
                <rest-connector socket-binding="rest">
                    <xsl:attribute name="cache-container">
                        <xsl:value-of select="$cache.container"/>
                    </xsl:attribute>
                    <authentication>
                        <xsl:attribute name="security-realm">
                            <xsl:value-of select="$security.realm"/>
                        </xsl:attribute>
                        <xsl:attribute name="auth-method">
                            <xsl:value-of select="$auth.method"/>
                        </xsl:attribute>
                    </authentication>
                </rest-connector>
            </xsl:when>
            <xsl:otherwise>
                <rest-connector socket-binding="rest">
                    <xsl:attribute name="cache-container">
                        <xsl:value-of select="$cache.container"/>
                    </xsl:attribute>
                </rest-connector>
                <rest-connector socket-binding="rest-ssl" name="rest-ssl">
                    <xsl:attribute name="cache-container">
                        <xsl:value-of select="$cache.container"/>
                    </xsl:attribute>
                    <authentication>
                        <xsl:attribute name="security-realm">
                            <xsl:value-of select="$security.realm"/>
                        </xsl:attribute>
                        <xsl:attribute name="auth-method">
                            <xsl:value-of select="$auth.method"/>
                        </xsl:attribute>
                    </authentication>
                    <encryption require-ssl-client-auth="true">
                        <xsl:attribute name="security-realm">
                            <xsl:value-of select="$security.realm"/>
                        </xsl:attribute>
                    </encryption>
                </rest-connector>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>

    <!-- Replace rest-connector element with new one - secured -->
    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsE)]/*[local-name()='rest-connector']">
        <xsl:copy-of select="$newRESTEndpointDefinition"/>
    </xsl:template>

    <xsl:template
            match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsE)]/*/@cache-container">
        <xsl:attribute name="cache-container">
            <xsl:value-of select="$cache.container"/>
        </xsl:attribute>
    </xsl:template>


    <!-- New DIGEST security-domain definition -->
    <xsl:variable name="newDigestSecurityDomainDefinition">
        <security-domain name="digest_auth" cache-type="infinispan">
            <authentication>
                <login-module code="UsersRoles" flag="required">
                    <module-option name="hashAlgorithm" value="MD5"/>
                    <module-option name="hashEncoding" value="rfc2617"/>
                    <module-option name="hashUserPassword" value="false"/>
                    <module-option name="hashStorePassword" value="true"/>
                    <module-option name="passwordIsA1Hash" value="true"/>
                    <module-option name="storeDigestCallback" value="org.jboss.security.auth.callback.RFC2617Digest"/>
                    <module-option name="usersProperties" value="${{jboss.server.config.dir}}/application-users.properties"/>
                    <module-option name="rolesProperties" value="${{jboss.server.config.dir}}/application-roles.properties"/>
                </login-module>
            </authentication>
        </security-domain>
    </xsl:variable>

    <!-- Add another security domain -->
    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsS)]
        /*[local-name()='security-domains' and starts-with(namespace-uri(), $nsS)]">
        <xsl:copy>
            <xsl:if test="$modifyDigestSecRealm != 'false'">
                <xsl:copy-of select="$newDigestSecurityDomainDefinition"/>
            </xsl:if>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

    <!-- New connector definition -->
    <xsl:variable name="newHttpsConnector">
        <connector name="https" protocol="HTTP/1.1" scheme="https" socket-binding="https" enable-lookups="false" secure="true">
            <ssl name="myssl"
                 keystore-type="JKS"
                 password="changeit"
                 key-alias="test"
                 truststore-type="JKS"
                 verify-client="want"
                 certificate-key-file="${{jboss.server.config.dir}}/server.keystore"
                 ca-certificate-file="${{jboss.server.config.dir}}/server.keystore"
                    />
        </connector>
    </xsl:variable>

    <!-- Add another connector -->
    <xsl:template match="//*[local-name()='subsystem' and starts-with(namespace-uri(), $nsW)]/*[local-name()='connector'][position()=last()]">
        <xsl:if test="$modifyCertSecRealm != 'false'">
            <xsl:copy>
                <xsl:apply-templates select="@*|node()"/>
            </xsl:copy>
            <xsl:copy-of select="$newHttpsConnector"/>
        </xsl:if>
    </xsl:template>

    <!-- Copy everything else. -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>

