<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!DOCTYPE xsl:stylesheet [
        <!ELEMENT xsl:stylesheet (xsl:output|xsl:key|xsl:template)*>
        <!ATTLIST xsl:stylesheet
                xmlns:xsl CDATA #REQUIRED
                xmlns CDATA #REQUIRED
                xmlns:html CDATA #REQUIRED
                xmlns:xs CDATA #REQUIRED
                version CDATA #REQUIRED
                exclude-result-prefixes CDATA #REQUIRED>
        <!ELEMENT xsl:output (#PCDATA)>
        <!ATTLIST xsl:output
                method CDATA #REQUIRED
                encoding CDATA #REQUIRED
                standalone CDATA #REQUIRED
                version CDATA #REQUIRED
                doctype-public CDATA #REQUIRED
                doctype-system CDATA #REQUIRED
                indent CDATA #REQUIRED>
        <!ELEMENT xsl:key (#PCDATA)>
        <!ATTLIST xsl:key
                match CDATA #REQUIRED
                name CDATA #REQUIRED
                use CDATA #REQUIRED>
        <!ELEMENT xsl:template (html|div|xsl:apply-templates|xsl:if|xsl:param|xsl:variable|xsl:choose|tr|xsl:for-each|a)*>
        <!ATTLIST xsl:template
                match CDATA #IMPLIED
                mode CDATA #IMPLIED
                name CDATA #IMPLIED>
        <!ELEMENT html (head|body)*>
        <!ELEMENT head (title|meta|style|script)*>
        <!ELEMENT title (xsl:value-of)*>
        <!ELEMENT xsl:value-of (#PCDATA)>
        <!ATTLIST xsl:value-of
                select CDATA #REQUIRED>
        <!ELEMENT meta (#PCDATA)>
        <!ATTLIST meta
                charset CDATA #REQUIRED>
        <!ELEMENT style (#PCDATA)>
        <!ELEMENT script (#PCDATA)>
        <!ATTLIST script
                src CDATA #IMPLIED
                type CDATA #REQUIRED>
        <!ELEMENT body (h1|xsl:apply-templates|div)*>
        <!ELEMENT h1 (xsl:value-of)*>
        <!ELEMENT xsl:apply-templates (xsl:with-param)*>
        <!ATTLIST xsl:apply-templates
                mode CDATA #IMPLIED
                select CDATA #REQUIRED>
        <!ELEMENT div (a|h3|div|p|xsl:choose|xsl:apply-templates)*>
        <!ATTLIST div
                class CDATA #IMPLIED
                id CDATA #IMPLIED>
        <!ELEMENT a (xsl:attribute|h3|xsl:value-of)*>
        <!ATTLIST a
                href CDATA #IMPLIED>
        <!ELEMENT h3 (xsl:value-of|xsl:apply-templates)*>
        <!ATTLIST h3
                class CDATA #IMPLIED>
        <!ELEMENT p (xsl:apply-templates)*>
        <!ELEMENT xsl:choose (xsl:when|xsl:otherwise)*>
        <!ELEMENT xsl:when (xsl:variable|xsl:value-of|xsl:apply-templates|xsl:call-template|xsl:choose|xsl:text)*>
        <!ATTLIST xsl:when
                test CDATA #REQUIRED>
        <!ELEMENT xsl:variable (xsl:choose)*>
        <!ATTLIST xsl:variable
                name CDATA #REQUIRED
                select CDATA #IMPLIED>
        <!ELEMENT xsl:otherwise (xsl:value-of|xsl:apply-templates|xsl:for-each|span|xsl:variable|xsl:call-template)*>
        <!ELEMENT xsl:if (table|xsl:apply-templates|xsl:value-of)*>
        <!ATTLIST xsl:if
                test CDATA #REQUIRED>
        <!ELEMENT table (tr|xsl:apply-templates)*>
        <!ATTLIST table
                class CDATA #IMPLIED>
        <!ELEMENT tr (th|td)*>
        <!ELEMENT th (#PCDATA)>
        <!ELEMENT xsl:with-param (#PCDATA)>
        <!ATTLIST xsl:with-param
                name CDATA #REQUIRED
                select CDATA #REQUIRED>
        <!ELEMENT xsl:param (#PCDATA)>
        <!ATTLIST xsl:param
                name CDATA #REQUIRED>
        <!ELEMENT xsl:for-each (xsl:apply-templates|xsl:call-template)*>
        <!ATTLIST xsl:for-each
                select CDATA #REQUIRED>
        <!ELEMENT xsl:attribute (xsl:value-of|xsl:apply-templates)*>
        <!ATTLIST xsl:attribute
                name CDATA #REQUIRED>
        <!ELEMENT td (xsl:value-of|xsl:choose|xsl:if|xsl:apply-templates|xsl:attribute)*>
        <!ELEMENT xsl:call-template (xsl:with-param)*>
        <!ATTLIST xsl:call-template
                name CDATA #REQUIRED>
        <!ELEMENT span (#PCDATA)>
        <!ATTLIST span
                class CDATA #REQUIRED>
        <!ELEMENT xsl:text (#PCDATA)>
        ]>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns="http://www.w3.org/1999/xhtml" xmlns:html="http://www.w3.org/1999/xhtml" xmlns:xs="http://www.w3.org/2001/XMLSchema" version="1.0" exclude-result-prefixes="xs html">
   <xsl:output method="html" encoding="ISO-8859-1" standalone="yes" version="1.0" doctype-public="-//W3C//DTD XHTML 1.0 Strict//EN" doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd" indent="yes" />

   <xsl:key name="simpleTypes" match="/xs:schema/xs:simpleType" use="@name"/>
   <xsl:key name="complexTypes" match="/xs:schema/xs:complexType" use="@name"/>
   <xsl:key name="imports" match="/xs:schema/xs:import" use="@namespace"/>


   <!-- Root -->
   <xsl:template match="/xs:schema">

      <html>
         <head>
            <title>
               <xsl:value-of select="@targetNamespace"/>
            </title>
            <meta charset="UTF-8" />
            <style>
               body { font-family: 'sans-serif'; }
               div.element, div.complexType { margin: 0 0 1em 1em; border-left: 5px solid #4477aa; border-bottom: 2px groove #4477aa;}
               table { border: 1px solid red; border-collapse: collapse; margin: 5px; width: 95%; }
               th { text-align: left; background: #eeeeee; color: #773333; }
               td, th { border: 1px solid gray; }
               h3 { border: 1px solid #4477aa; background: #eeeeee; color: #4477aa; margin: 0.3em 0 0 0; padding: 0.3em; }
               a { text-decoration: none; color: #4477aa; }
               a:hover { color: white; background: #4477aa; }
               .error { color: white; background: red; }
               .content { }
               .heading { cursor: pointer; }
               .complexType, .simpleType {  }
               table.enumeration { border: 0; margin: 0; }
               #global {
                  position: fixed;
                  top: 1em;
                  right: 1em;
                  border: 1px solid #4477aa;
                  border-bottom: 2px groove #4477aa;
                  z-index: 100;
                  background: white;
                  padding: 0.5em;
               }
            </style>
            <script type="text/javascript">
               (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
               (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
               m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
               })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');

               ga('create', 'UA-8601422-4', 'auto');
               ga('send', 'pageview');
            </script>
            <script
                    src="https://code.jquery.com/jquery-2.2.4.min.js"
                    integrity="sha256-BbhdlvQf/xTY9gja0Dq3HiwQF8LaCRTXxZKRutelT44="
                    crossorigin="anonymous">
            </script>
            <script type="text/javascript">
               $(document).ready(function() {
                 $(".heading").click(function() {
                   $(this).next(".content").slideToggle(500);
                 });
                 $("#global a").click(function() {
                   $(".content").toggle();
                 });
               });
            </script>
         </head>
         <body>
            <h1>
               <xsl:value-of select="@targetNamespace"/>
            </h1>

            <xsl:apply-templates select="xs:element" />

            <div id="global">
               <a href="#">Expand/Collapse All</a>
            </div>
         </body>
      </html>
   </xsl:template>

   <xsl:template match="xs:element">
      <div class="element">
         <h3 class="heading">
            <xsl:value-of select="@name" />
            <xsl:apply-templates select="." mode="occurs" />
         </h3>
         <div class="content">
            <p><xsl:apply-templates select="xs:annotation" /></p>
            <xsl:choose>
               <xsl:when test="@type">
                  <xsl:variable name="ref">
                     <xsl:choose>
                        <xsl:when test="contains(string(@type), ':')">
                           <xsl:value-of select="substring-after(string(@type), ':')" />
                        </xsl:when>
                        <xsl:otherwise>
                           <xsl:value-of select="string(@type)" />
                        </xsl:otherwise>
                     </xsl:choose>
                  </xsl:variable>
                  <xsl:apply-templates select="key('complexTypes', $ref)" />
               </xsl:when>
               <xsl:otherwise>
                  <xsl:apply-templates select="xs:complexType" />
               </xsl:otherwise>
            </xsl:choose>
         </div>
      </div>
   </xsl:template>

   <xsl:template match="xs:complexType">
      <xsl:apply-templates select="xs:annotation" />

      <xsl:if test="xs:attribute">
         <table>
            <tr>
               <th>Name</th>
               <th>Type</th>
               <th>Default</th>
               <th>Description</th>
            </tr>
            <xsl:apply-templates select="xs:attribute" />
         </table>
      </xsl:if>
      <xsl:if test="xs:all | xs:sequence | xs:complexContent | xs:choice">
         <xsl:apply-templates select="xs:all | xs:sequence | xs:complexContent | xs:choice" />
      </xsl:if>
   </xsl:template>

   <xsl:template match="xs:complexContent">
      <xsl:apply-templates select="xs:extension"/>
   </xsl:template>

   <xsl:template match="xs:extension">
      <xsl:apply-templates select="xs:annotation" />

      <xsl:if test="xs:attribute">
         <table>
            <tr>
               <th>Name</th>
               <th>Type</th>
               <th>Default</th>
               <th>Description</th>
            </tr>
            <xsl:apply-templates select="xs:attribute" />
         </table>
      </xsl:if>
      <xsl:if test="xs:all | xs:sequence | xs:complexContent">
         <xsl:apply-templates select="xs:all | xs:sequence | xs:complexContent" />
      </xsl:if>
      <div>
         <xsl:apply-templates select="/xs:schema" mode="lookup-type">
            <xsl:with-param name="type" select="string(@base)" />
         </xsl:apply-templates>
      </div>
   </xsl:template>

   <xsl:template match="xs:schema" mode="lookup-type">
      <xsl:param name="type" />
      <xsl:variable name="ref">
         <xsl:choose>
            <xsl:when test="contains(string($type), ':')">
               <xsl:value-of select="substring-after(string($type), ':')" />
            </xsl:when>
            <xsl:otherwise>
               <xsl:value-of select="string($type)" />
            </xsl:otherwise>
         </xsl:choose>
      </xsl:variable>
      <xsl:choose>
         <xsl:when test="key('complexTypes',$ref)">
            <xsl:apply-templates select="key('complexTypes',$ref)" />
         </xsl:when>
         <xsl:otherwise>
            <xsl:for-each select="/xs:schema/xs:import">
               <xsl:apply-templates select="document(@schemaLocation, .)/xs:schema" mode="lookup-type">
                  <xsl:with-param name="type" select="$type"/>
               </xsl:apply-templates>
            </xsl:for-each>
         </xsl:otherwise>
      </xsl:choose>


   </xsl:template>

   <xsl:template match="xs:complexType" mode="top-level">
      <div class="complexType">
         <a>
            <xsl:attribute name="id"><xsl:value-of select="@name" /></xsl:attribute>
            <h3><xsl:value-of select="@name" /></h3>
         </a>
         <xsl:apply-templates select="." />
      </div>
   </xsl:template>

   <xsl:template match="xs:all">
      <xsl:apply-templates select="xs:element | xs:choice" />
   </xsl:template>

   <xsl:template match="xs:any">
      <xsl:apply-templates select="xs:element | xs:choice" />
   </xsl:template>

   <xsl:template match="xs:sequence">
      <xsl:apply-templates select="xs:element | xs:choice" />
   </xsl:template>

   <xsl:template match="xs:attribute">
      <tr>
         <td>
            <xsl:value-of select="@name" />
         </td>
         <td>
            <xsl:choose>
               <xsl:when test="xs:simpleType">
                  <xsl:apply-templates select="xs:simpleType" mode="embedded"/>
               </xsl:when>
               <xsl:when test="@type">
                  <xsl:call-template name="print-type">
                     <xsl:with-param name="ref" select="@type" />
                  </xsl:call-template>
               </xsl:when>
               <xsl:otherwise>
                  <span class="error">FIXME</span>
               </xsl:otherwise>
            </xsl:choose>
         </td>
         <td>
            <xsl:if test="@default">
               <xsl:value-of select="@default"></xsl:value-of>
            </xsl:if>
         </td>
         <td>
            <xsl:apply-templates select="xs:annotation" />
         </td>
      </tr>
   </xsl:template>

   <xsl:template match="xs:annotation">
      <xsl:for-each select="xs:documentation">
         <xsl:call-template name="format-text">
            <xsl:with-param name="text" select="."/>
         </xsl:call-template>
      </xsl:for-each>
   </xsl:template>

   <xsl:template name="print-type">
      <xsl:param name="ref" />

      <xsl:choose>
         <xsl:when test="starts-with($ref, 'xs:')">
            <!-- standard XSD types -->
            <xsl:value-of select="substring-after($ref, ':')" />
         </xsl:when>
         <xsl:otherwise>
            <xsl:apply-templates select="key('simpleTypes', substring-after($ref, ':'))" mode="embedded"/>
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>

   <xsl:template match="xs:simpleType" mode="embedded">
      <xsl:apply-templates select="xs:restriction | xs:list" />
   </xsl:template>

   <xsl:template match="xs:simpleType" mode="top-level">
      <div class="simpleType">
         <a>
            <xsl:attribute name="id"><xsl:value-of select="@name" /></xsl:attribute>
            <h3>
               <xsl:value-of select="@name" />
            </h3>
         </a>
         <xsl:apply-templates select="xs:restriction | xs:list" />
      </div>
   </xsl:template>

   <xsl:template match="xs:restriction">
      <xsl:if test="xs:enumeration">
         <table class="enumeration">
            <xsl:apply-templates select="xs:enumeration" />
         </table>
      </xsl:if>
   </xsl:template>

   <xsl:template match="xs:list">
      <xsl:variable name="ref" select="substring-after(string(@itemType), ':')" />
      <xsl:apply-templates select="key('simpleTypes', $ref)" mode="embedded"/>
   </xsl:template>

   <xsl:template match="xs:enumeration">
      <tr>
         <td>
            <xsl:attribute name="title"><xsl:apply-templates select="xs:annotation" /></xsl:attribute>
            <xsl:value-of select="@value" />
         </td>
         <td><xsl:apply-templates select="xs:annotation" /></td>
      </tr>
   </xsl:template>

   <xsl:template match="xs:element | xs:any | xs:all | xs:sequence | xs:choice | xs:group" mode="occurs">
      <xsl:variable name="minOccurs">
         <xsl:choose>
            <xsl:when test="@minOccurs = 'unbounded'">*</xsl:when>
            <xsl:when test="@minOccurs">
               <xsl:value-of select="@minOccurs" />
            </xsl:when>
            <xsl:otherwise>1</xsl:otherwise>
         </xsl:choose>
      </xsl:variable>
      <xsl:variable name="maxOccurs">
         <xsl:choose>
            <xsl:when test="@maxOccurs = 'unbounded'">*</xsl:when>
            <xsl:when test="@maxOccurs">
               <xsl:value-of select="@maxOccurs" />
            </xsl:when>
            <xsl:otherwise>1</xsl:otherwise>
         </xsl:choose>
      </xsl:variable>
      <xsl:choose>
         <xsl:when test="$minOccurs = '0' and $maxOccurs = '1'">?</xsl:when>
         <xsl:when test="$minOccurs = '1' and $maxOccurs = '1'"></xsl:when>
         <xsl:when test="$minOccurs = '0' and $maxOccurs = '*'">*</xsl:when>
         <xsl:when test="$minOccurs = '1' and $maxOccurs = '*'">+</xsl:when>
         <xsl:otherwise>
            <xsl:value-of select="concat('{', $minOccurs, ',', $maxOccurs, '}')" />
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>

   <xsl:template name="format-text">
      <xsl:param name="text" />
      <xsl:choose>
         <xsl:when test="contains($text, 'http://')">
            <xsl:variable name="after_scheme" select="substring-after($text, 'http://')" />
            <xsl:value-of select="substring-before($text, 'http://')" />
            <xsl:choose>
               <xsl:when test="contains($after_scheme, ' ')">
                  <xsl:variable name="url" select="concat('http://', substring-before($after_scheme, ' '))" />
                  <xsl:call-template name="linkify">
                     <xsl:with-param name="url" select="$url" />
                  </xsl:call-template>
                  <xsl:text> </xsl:text>
                  <xsl:call-template name="format-text">
                     <xsl:with-param name="text" select="substring-after($after_scheme, ' ')" />
                  </xsl:call-template>
               </xsl:when>
               <xsl:otherwise>
                  <xsl:variable name="url" select="concat('http://', $after_scheme)" />
                  <xsl:call-template name="linkify">
                     <xsl:with-param name="url" select="$url" />
                  </xsl:call-template>
               </xsl:otherwise>
            </xsl:choose>
         </xsl:when>
         <xsl:when test="contains($text, 'https://')">
            <xsl:variable name="after_scheme" select="substring-after($text, 'https://')" />
            <xsl:value-of select="substring-before($text, 'https://')" />
            <xsl:choose>
               <xsl:when test="contains($after_scheme, ' ')">
                  <xsl:variable name="url" select="concat('https://', substring-before($after_scheme, ' '))" />
                  <xsl:call-template name="linkify">
                     <xsl:with-param name="url" select="$url" />
                  </xsl:call-template>
                  <xsl:text> </xsl:text>
                  <xsl:call-template name="format-text">
                     <xsl:with-param name="text" select="substring-after($after_scheme, ' ')" />
                  </xsl:call-template>
               </xsl:when>
               <xsl:otherwise>
                  <xsl:variable name="url" select="concat('https://', $after_scheme)" />
                  <xsl:call-template name="linkify">
                     <xsl:with-param name="url" select="$url" />
                  </xsl:call-template>
               </xsl:otherwise>
            </xsl:choose>
         </xsl:when>
         <xsl:otherwise>
            <xsl:value-of select="$text" />
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>

   <xsl:template name="linkify">
      <xsl:param name="url" />

      <a>
         <xsl:attribute name="href">
            <xsl:value-of select="$url" />
         </xsl:attribute>
         <xsl:value-of select="$url" />
      </a>
   </xsl:template>
</xsl:stylesheet>
