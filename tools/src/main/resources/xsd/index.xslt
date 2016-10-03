<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns="http://www.w3.org/1999/xhtml" xmlns:html="http://www.w3.org/1999/xhtml" version="1.0" exclude-result-prefixes="html">
   <xsl:output method="xml" encoding="ISO-8859-1" standalone="yes" version="1.0" doctype-public="-//W3C//DTD XHTML 1.0 Strict//EN" doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd" indent="yes" />
   <!-- Root -->
   <xsl:template match="/files">
      <html>
         <head>
            <title>
               Infinispan Configuration Schemas
            </title>
            <meta charset="UTF-8" />
            <style>
               body { font-family: 'sans-serif'; }
               a { text-decoration: none; color: #4477aa; }
               a:hover { color: white; background: #4477aa; }
            </style>
            <script type="text/javascript">
               (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
               (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
               m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
               })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');

               ga('create', 'UA-8601422-4', 'auto');
               ga('send', 'pageview');
            </script>
         </head>
         <body>
            <h1>
               Infinispan Configuration Schemas
            </h1>

            <ul>
                <xsl:apply-templates select="file" />
            </ul>
         </body>
      </html>
   </xsl:template>

   <xsl:template match="file">
      <li>
        <a>
           <xsl:attribute name="href"><xsl:value-of select="@name" /></xsl:attribute>
           <xsl:value-of select="@ns" />
        </a>
      </li>
   </xsl:template>
</xsl:stylesheet>
