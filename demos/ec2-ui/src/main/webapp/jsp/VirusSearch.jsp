<%-- 
    Document   : VirusSearch
    Author     : noconnor@redhat.com
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html>
   <head>
      <meta content="text/html; charset=UTF-8" http-equiv="Content-Type">
      <meta content="Infinispan is a highly scalable platform for distributed data grids." name="description">
      <meta content="Scalable, jsr107, cache, caching, distributed, grid, data grid, infinispan, nosql" name="keywords">
      <meta content="all" name="robots">
      <meta content="no-cache" http-equiv="cache-control">
      <meta content="no-cache" http-equiv="pragma">
      <link href="http://jboss.org/infinispan/orgLayoutBody/favicon/favicon.png" rel="Shortcut Icon">
      <title>Virus Details Search</title>
      <style type="text/css">
         body{
            background: #CCCCCC;
            font-family: 'Lucida Sans', 'Lucida Sans Unicode', 'Lucida Grande', Verdana, Arial, Helvetica, sans-serif;
            font-size: 12px;
            padding: 10px auto 10px auto;

            }

         .main{
            width: 600px;
            background: white;
            padding: 30px 30px 30px 30px;
            align: left;
            margin: 0px auto 0px auto;
            -moz-border-radius: 12px;
  	         -webkit-border-radius: 12px;
  	         -khtml-border-radius: 12px;
  	         border-radius: 12px;
            }

         h1{font-weight: bold; font-size: 2.2em; display: block; color: #991133;}
         h2{font-weight: bold; font-size: 1.8em; display: block; color: #4A5D75;}
         h3{font-weight: bold; font-size: 1.6em; display: block; color: #4A5D75;}
         h4{font-weight: bold; font-size: 1.5em; display: block; color: #4A5D75;}
         h5{font-weight: bold; font-size: 1.3em; display: block; color: #4A5D75;}
         h6{font-weight: bold; font-size: 1.2em; display: block; color: #4A5D75;}

         a:link, a:visited {
            color: #355491;
            text-decoration: none;
         }

         a:hover {
            color: #355491;
            text-decoration: underline;
         }

         tt {font-family: "Courier New", Courier, mono; color: #444444; font-weight: bold; padding: 0 2px 0 2px;}
         table {align: center; border: 1px #404040 solid;}
         th {background: #404040; color: #cccccc; text-align: left; border: 1px #404040 solid; padding: 4px 4px 4px 4px;}
         td {text-align: left; border: 1px #404040 solid; padding: 4px 4px 4px 4px;}
    </style>
   </head>

   <body>
   <p align="center">
      <a href="http://www.infinispan.org"><img src="banner.png" border="0"></a>
        </p>
     <div class="main">
     <h1>Enter Virus GBAN</h1>
      <form action="/infinispan-ec2-demoui/CacheSearchServlet" method="POST">
      Genbank Accession Number <INPUT TYPE="TEXT" NAME="vGBAN"> <INPUT type="submit" value="Go" /></form>
</div></body></html>
