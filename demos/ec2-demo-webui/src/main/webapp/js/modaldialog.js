///// downloads box implementation /////

var HEADER = "Reminder";
var TEXT_1 = "Community projects represent the latest development releases and are ";
var UNDERLINED_1 = "not"; 
var TEXT_1a = " supported.";
var TEXT_2 = "If you're looking for fully supported, certified, enterprise middleware try ";
var LINKTEXT_1 = "JBoss Enterprise Middleware";
var TEXT_2a = " products. \u00a0\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0";
var DETAILS = "Show Details";

var ENTERPRISE_LINK = "http://www.jboss.com/products";
var DETAILS_LINK = "http://www.jboss.com/products/community-enterprise/";

//var TEXT_3 = ".";
//var TEXT_4 = "Please note that Red Hat ";
//var BOLD_1 = "does not";
//var TEXT_5 = " provide support for this software but ";
//var BOLD_2 = "does";
//var TEXT_6 = "  offer fully-supported, enterprise ready solutions based on it. For more details see ";

var JBOSSCOM = "Show Details";
//var TEXT_7 = ".";
var CONTINUE = "Continue Download";
//var MORE_INFO = "More Info...";
var CANCEL = "Cancel Download";

//var JBOSSCOM_LINK = "http://www.jboss.com";
//var FORUMS_LINK = "http://www.jboss.com/index.html?module=bb";
//
//var FORUMS = "FORUMS FORUMS FORUMS";

function turnOnModal(downloadAnhor) {

   tkOnModal();

   createDownloadsBox(downloadAnhor.href);
}

// Based upon http://slayeroffice.com/code/custom_alert
function createDownloadsBox(url) {

	// if the modallayer object already exists in the DOM, bail out.
	if(document.getElementById("modallayer")) return;

	// create the modallayer div as a child of the BODY element
	var mObj = document.getElementsByTagName("body")[0].appendChild(document.createElement("div"));
	mObj.id = "modallayer";
	 // make sure its as tall as it needs to be to overlay all the content on the page
	mObj.style.height = document.documentElement.scrollHeight + "px";

	// create the DIV that will be the alert
	var alertObj = mObj.appendChild(document.createElement("div"));
	alertObj.id = "dialog";
	// MSIE doesnt treat position:fixed correctly, so this compensates for positioning the alert
	if(document.all && !window.opera) alertObj.style.top = document.documentElement.scrollTop + "px";
	// center the alert box
	alertObj.style.left = (document.documentElement.scrollWidth - alertObj.offsetWidth)/2 + "px";

   var div = alertObj.appendChild(document.createElement("div"));

   var h = div.appendChild(document.createElement("h4"));
   h.appendChild(document.createTextNode(HEADER));

   var p1 = div.appendChild(document.createElement("p"));
   p1.style.fontWeight = "bold";
   p1.appendChild(document.createTextNode(TEXT_1));
   var u1 = document.createElement("u");
   u1.appendChild(document.createTextNode(UNDERLINED_1));
   p1.appendChild(u1);
   p1.appendChild(document.createTextNode(TEXT_1a));
   
   var p2 = div.appendChild(document.createElement("p"));
   p2.appendChild(document.createTextNode(TEXT_2));

   var a = p2.appendChild(document.createElement("a"));
   a.appendChild(document.createTextNode(LINKTEXT_1));
   a.href = ENTERPRISE_LINK;
   a.onclick = function() { turnOffModal();tkForums();return true; } ;
   
   p2.appendChild(document.createTextNode(TEXT_2a));

   var a2 = p2.appendChild(document.createElement("a"));
   a2.appendChild(document.createTextNode(DETAILS));
   a2.href = DETAILS_LINK;
   a2.onclick = function() { turnOffModal();tkJBossCom();return true; } ;

//   var p3 = div.appendChild(document.createElement("p"));
//   p3.appendChild(document.createTextNode(TEXT_4));
//
//   var b = p3.appendChild(document.createElement("b"));
//   b.appendChild(document.createTextNode(BOLD_1));
//
//   p3.appendChild(document.createTextNode(TEXT_5));
//   
//   var b2 = p3.appendChild(document.createElement("b"));
//   b2.appendChild(document.createTextNode(BOLD_2));
//
//   p3.appendChild(document.createTextNode(TEXT_6));
//
//
//   p3.appendChild(document.createTextNode(TEXT_7));

   var btnrow = alertObj.appendChild(document.createElement("div"));
   btnrow.id = "buttonrow";

   var ul = btnrow.appendChild(document.createElement("ul"));

   ul.appendChild(createLi(CONTINUE, "continue", url, function() { turnOffModal();tkContinue();return true; } ));
   //   ul.appendChild(createLi(MORE_INFO, "", ENTERPRISE_LINK, function() { turnOffModal();tkMoreInfo();return true; } ));
   ul.appendChild(createLi(CANCEL, "cancel", "#", function() { turnOffModal();tkCancel();return false; }));

}

function createLi(text, id, url, onclick) {

   var li = document.createElement("li");

   var a = li.appendChild(document.createElement("a"));
   a.appendChild(document.createTextNode(text));
   a.className = "hrefbuttons";
   a.onclick = onclick;
   a.href = url;
   a.id = id;

   return li;
}

function turnOffModal() {

   document.getElementsByTagName("body")[0].removeChild(document.getElementById("modallayer"));
}

///// tracking /////

function tk(suffix) {

   // Google Analytics
   //pageTracker._trackPageview(location.pathname + "/" + suffix);

   // Omniture SiteCatalyst
   s.pageName = location.pathname + "/" + suffix;
   void(s.t());
}

function tkOnModal() {
  tk("open");
}

function tkCancel() {
  tk("cancel");
}

function tkContinue() {
  tk("continue");
}

function tkMoreInfo() {
  tk("moreinfo");
}

function tkForums() {
  tk("forums");
}

function tkJBossCom() {
  tk("jbosscom");
}

///// initailization /////

function init() {

   var elements = getElementsByStyleClass("rowline");

   for (var e = 0; e < elements.length; e++) {

      var children = elements[e].childNodes;
      for (var c = 0; c < children.length; c++) {
         var child = children[c];
         if ((child != null) && (((child.nodeName == "A") || (child.nodeName == "a"))
                          && (child.firstChild != null)
                          && (child.firstChild.nodeValue != null)
                          && (child.firstChild.nodeValue.toLowerCase().indexOf("download") != -1))) {
            child.onclick = function() { turnOnModal(this); return false; };
         }
      }
   }
}

function getElementsByStyleClass (className) {
  var all = document.all ? document.all :
    document.getElementsByTagName('*');
  var elements = new Array();
  for (var e = 0; e < all.length; e++)
    if ((all[e].className != null) && (all[e].className.indexOf(className) != -1)) {
      elements[elements.length] = all[e];
    }
  return elements;
}

// menu appendices
function addJiraLink(jiralink) {
	// if no link is provided, do nothing
	if (jiralink==null || jiralink=='') return;
	// if item already exists, do nothing
	if (document.getElementById("jiralinkitem")) return;
	// else
    var myLI = document.getElementById("issuetrackermenu"); 
    var children = myLI.childNodes;

    var jira = document.createElement("li");
    jira.className = "leaf";
    jira.id = "jiralinkitem"
    var link = jira.appendChild(document.createElement("a"));
    link.href = jiralink;
    link.appendChild(document.createTextNode("JIRA"));

    var myUL = document.createElement("ul");
    myUL.className = "level1";
    myUL.appendChild(jira);
    
    for (var c=0; c < children.length; c++) {
      var child = children[c];
      if (child!=null && (child.nodeName == 'ul' || child.nodeName == 'UL')) {
        var itms = child.childNodes;
        var itmsnum = itms.length;

        for (var i=0; i < itmsnum; ) {
          myUL.appendChild(itms[0]);
          i++;
        }
        
        myLI.replaceChild(myUL, child);
      } else {
    	myLI.appendChild(myUL);  
      }
    }
}

function addHudsonLink(hudsonlink) {
	// if no link is provided, do nothing
	if (hudsonlink==null || hudsonlink=='') return;
	// if item already exists, do nothing
	if (document.getElementById("hudsonlinkitem")) return;
	// else
    var myLI = document.getElementById("buildmenu"); 
    var children = myLI.childNodes;

    var hudson = document.createElement("li");
    hudson.className = "leaf";
    hudson.id = "hudsonlinkitem"
    var link = hudson.appendChild(document.createElement("a"));
    link.href = hudsonlink;
    link.appendChild(document.createTextNode("Hudson"));

    var myUL = document.createElement("ul");
    myUL.className = "level1";
    myUL.appendChild(hudson);

    for (var c=0; c < children.length; c++) {
      var child = children[c];

      
      if (child!=null && (child.nodeName == 'ul' || child.nodeName == 'UL')) {
        var itms = child.childNodes;
        var itmsnum = itms.length;
        
        for (var i=0; i < itmsnum; ) {
          myUL.appendChild(itms[0]);
          i++;
        }
        
        myLI.replaceChild(myUL, child);
      } else {
    	myLI.appendChild(myUL);  
      }
    }
}


// Trigger initialization at startup
init();
