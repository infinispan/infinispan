<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:security="urn:jboss:domain:security:1.2"
                xmlns:web="urn:jboss:domain:web:1.1"
                xmlns:endpoint="urn:infinispan:server:endpoint:6.0">

    <xsl:output method="xml" indent="yes" omit-xml-declaration="yes"/>

    <xsl:param name="security.domain" select="'other'"/>
    <xsl:param name="security.mode" select="'WRITE'"/>
    <xsl:param name="auth.method" select="'BASIC'"/>
    <xsl:param name="cache.container" select="'${connector.cache.container}'"/>
    <xsl:param name="modifyCertSecDomain" select="false"/>
    <xsl:param name="modifyDigestSecDomain" select="false"/>

    <!-- New rest-connector definition -->
    <xsl:variable name="newRESTEndpointDefinition">
        <rest-connector virtual-server="default-host">
            <xsl:attribute name="cache-container">
                <xsl:value-of select="$cache.container"/>
            </xsl:attribute>
            <xsl:attribute name="security-domain">
                <xsl:value-of select="$security.domain"/>
            </xsl:attribute>
            <xsl:attribute name="auth-method">
                <xsl:value-of select="$auth.method"/>
            </xsl:attribute>
            <xsl:attribute name="security-mode">
                <xsl:value-of select="$security.mode"/>
            </xsl:attribute>
        </rest-connector>
    </xsl:variable>

    <!-- Replace rest-connector element with new one - secured -->
    <xsl:template match="//endpoint:subsystem/endpoint:rest-connector">
        <xsl:copy-of select="$newRESTEndpointDefinition"/>
    </xsl:template>

    <xsl:template match="//endpoint:subsystem/endpoint:hotrod-connector/@cache-container">
        <xsl:attribute name="cache-container">
            <xsl:value-of select="$cache.container"/>
        </xsl:attribute>
    </xsl:template>

    <xsl:template match="//endpoint:subsystem/endpoint:memcached-connector/@cache-container">
        <xsl:attribute name="cache-container">
            <xsl:value-of select="$cache.container"/>
        </xsl:attribute>
    </xsl:template>

    <xsl:template match="//endpoint:subsystem/endpoint:websocket-connector/@cache-container">
        <xsl:attribute name="cache-container">
            <xsl:value-of select="$cache.container"/>
        </xsl:attribute>
    </xsl:template>

    <!-- New CERT security-domain definition -->
    <xsl:variable name="newClientCertSecurityDomainDefinition">
        <security:security-domain name="client_cert_auth" cache-type="infinispan">
            <security:authentication>
                <security:login-module code="CertificateRoles" flag="required">
                    <security:module-option name="securityDomain" value="client_cert_auth"/>
                    <security:module-option name="rolesProperties">
                        <xsl:attribute name="value">
                            <xsl:text disable-output-escaping="no">${jboss.server.config.dir}/roles.properties</xsl:text>
                        </xsl:attribute>
                    </security:module-option>
                </security:login-module>
            </security:authentication>
            <security:jsse truststore-password="changeit" client-auth="true">
                <xsl:attribute name="truststore-url">
                    <xsl:text disable-output-escaping="no">${jboss.server.config.dir}/jsse.keystore</xsl:text>
                </xsl:attribute>
            </security:jsse>
        </security:security-domain>
    </xsl:variable>

    <!-- New DIGEST security-domain definition -->
    <xsl:variable name="newDigestSecurityDomainDefinition">
        <security:security-domain name="digest_auth" cache-type="infinispan">
            <security:authentication>
                <security:login-module code="UsersRoles" flag="required">
                    <security:module-option name="hashAlgorithm" value="MD5"/>
                    <security:module-option name="hashEncoding" value="rfc2617"/>
                    <security:module-option name="hashUserPassword" value="false"/>
                    <security:module-option name="hashStorePassword" value="true"/>
                    <security:module-option name="passwordIsA1Hash" value="true"/>
                    <security:module-option name="storeDigestCallback" value="org.jboss.security.auth.callback.RFC2617Digest"/>
                    <security:module-option name="usersProperties">
                        <xsl:attribute name="value">
                            <xsl:text
                                    disable-output-escaping="no">${jboss.server.config.dir}/application-users.properties</xsl:text>
                        </xsl:attribute>
                    </security:module-option>
                    <security:module-option name="rolesProperties">
                        <xsl:attribute name="value">
                            <xsl:text
                                    disable-output-escaping="no">${jboss.server.config.dir}/application-roles.properties</xsl:text>
                        </xsl:attribute>
                    </security:module-option>
                </security:login-module>
            </security:authentication>
        </security:security-domain>
    </xsl:variable>

    <!-- New connector definition -->
    <xsl:variable name="newHttpsConnector">
        <web:connector name="https" protocol="HTTP/1.1" scheme="https" socket-binding="https" enable-lookups="false"
                       secure="true">
            <web:ssl name="myssl"
                     keystore-type="JKS"
                     password="changeit"
                     key-alias="test"
                     truststore-type="JKS"
                     verify-client="want">
                <xsl:attribute name="certificate-key-file">
                    <xsl:text disable-output-escaping="no">${jboss.server.config.dir}/server.keystore</xsl:text>
                </xsl:attribute>
                <xsl:attribute name="ca-certificate-file">
                    <xsl:text disable-output-escaping="no">${jboss.server.config.dir}/server.keystore</xsl:text>
                </xsl:attribute>
            </web:ssl>
        </web:connector>
    </xsl:variable>

    <!-- Add another connector -->
    <xsl:template match="//web:subsystem/web:connector[position()=last()]">
        <xsl:if test="$modifyCertSecDomain != 'false'">
            <xsl:copy>
                <xsl:apply-templates select="@*|node()"/>
            </xsl:copy>
            <xsl:copy-of select="$newHttpsConnector"/>
        </xsl:if>
    </xsl:template>

    <!-- Add another security domain -->
    <xsl:template match="//security:subsystem/security:security-domains/security:security-domain[position()=last()]">
        <xsl:if test="$modifyCertSecDomain != 'false'">
            <xsl:copy>
                <xsl:apply-templates select="@*|node()"/>
            </xsl:copy>
            <xsl:copy-of select="$newClientCertSecurityDomainDefinition"/>
        </xsl:if>

        <xsl:if test="$modifyDigestSecDomain != 'false'">
            <xsl:copy>
                <xsl:apply-templates select="@*|node()"/>
            </xsl:copy>
            <xsl:copy-of select="$newDigestSecurityDomainDefinition"/>
        </xsl:if>

    </xsl:template>

    <!-- Copy everything else. -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>

