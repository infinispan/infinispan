function submitonEnter(e){
	
    var keynum;
    var keychar;
    var numcheck;

    if (window.event) {
        keynum = e.keyCode;
    } else if (e.which) {
        keynum = e.which;
    }

    if (keynum == "13"){
        document.searchBox.submit();
    }
}

function searchCommunity(){
	resetSelected();
	jQuery("#q").val("Search the Community");
	jQuery("#searchArea").val(jQuery("#q").val());
	search[0]='<a class="selected" id="searchCommunity" href="javascript:void(0);" onclick="searchCommunity()">Search the Community</a>';
  document.getElementById("TopSearch").method="POST";
  document.getElementById("TopSearch").action='/community/search?&resultTypes=DOCUMENT&resultTypes=COMMUNITY&resultTypes=COMMENT&peopleEnabled=true';
}

function searchJBossorgSite(){
	resetSelected();
	jQuery("#q").val("Search the JBoss.org site");
	jQuery("#searchArea").val(jQuery("#q").val());
	search[0]='<a class="selected" id="searchJBossorgSite" href="javascript:void(0);" onclick="searchJBossorgSite()">Search the JBoss.org site</a>';
  document.getElementById("TopSearch").method="GET";
	document.getElementById("TopSearch").action='http://www.google.com/search?&as_sitesearch=jboss.org';
}

function searchProjectPages(){
	resetSelected();
	jQuery("#q").val("Search Project Pages");
	jQuery("#searchArea").val(jQuery("#q").val());
	search[1]='<a class="selected" id="searchProjectPages" href="javascript:void(0);" onclick="searchProjectPages()">Search Project Pages</a>';
  document.getElementById("TopSearch").method="GET";
	document.getElementById("TopSearch").action='http://www.google.com/search?&as_sitesearch=jboss.org';
}

function resetSelected(){
	//search[0]='<a id="searchJBossorgSite" href="javascript:void(0);" onclick="searchJBossorgSite()">Search the JBoss.org site</a>'
	search[0]='<a id="searchCommunity" href="javascript:void(0);" onclick="searchCommunity()">Search the Community</a>'
	search[1]='<a id="searchProjectPages" href="javascript:void(0);" onclick="searchProjectPages()">Search Project Pages</a>'
	//search[4]='<img src="/jbossorg-search/img/search-filter-bottom.png" width="133" height="7" style="margin-top:-6px;" class="pngfix" />'
}

var search=new Array()
//search[0]='<a id="searchJBossorgSite" href="javascript:void(0);" onclick="searchJiveSite()">Search the JBoss.org site</a>'
search[0]='<a id="searchCommunity" href="javascript:void(0);" onclick="searchCommunity()">Search the Community</a>'
search[1]='<a id="searchProjectPages" href="javascript:void(0);" onclick="searchDocumentation()">Search Project Pages</a>'
//search[4]='<img src="/jbossorg-search/img/search-filter-bottom.png" width="133" height="7" style="margin-top:-6px;" class="pngfix" />'

function setSelected(){
	if (jQuery("#searchArea").val() == "Search the JBoss.org site") {
		searchJBossorgSite();
		return;
	}

	if (jQuery("#searchArea").val() == "Search Project Pages") {
		searchProjectPages();
		return;
	}

	if (jQuery("#searchArea").val() == "Search the Community") {
		searchCommunity();
		return;
	}

	try{ // if default search to community is defined 
		if (defaultSearchToCommunity==true){
			searchCommunity();
			return;
		}
	}catch(err){
	}

	if (jQuery("#searchArea").val()=="") {
		searchCommunity();
	}

}
jQuery(document).ready(
	function () {
		setSelected();
	}
);

var menuwidth='140px'
var menubgcolor=''
var disappeardelay=250
var hidemenu_onclick="yes"

var ie4=document.all
var ns6=document.getElementById&&!document.all

if (ie4||ns6)
document.write('<div id="dropmenudiv" style="visibility:hidden;width:'+menuwidth+';background-color:'+menubgcolor+'" onMouseover="clearhidemenu()" onMouseout="dynamichide(event)"></div>')

function getposOffset(what, offsettype){
var totaloffset=(offsettype=="left")? what.offsetLeft : what.offsetTop;
var parentEl=what.offsetParent;
while (parentEl!=null){
totaloffset=(offsettype=="left")? totaloffset+parentEl.offsetLeft : totaloffset+parentEl.offsetTop;
parentEl=parentEl.offsetParent;
}
return totaloffset;
}


function showhide(obj, e, visible, hidden, menuwidth){
if (ie4||ns6)
dropmenuobj.style.left=dropmenuobj.style.top="-500px"
if (menuwidth!=""){
dropmenuobj.widthobj=dropmenuobj.style
dropmenuobj.widthobj.width=menuwidth
}
if (e.type=="click" && obj.visibility==hidden || e.type=="mouseover")
obj.visibility=visible
else if (e.type=="click")
obj.visibility=hidden
}

function iecompattest(){
return (document.compatMode && document.compatMode!="BackCompat")? document.documentElement : document.body
}

function clearbrowseredge(obj, whichedge){
var edgeoffset=0
if (whichedge=="rightedge"){
var windowedge=ie4 && !window.opera? iecompattest().scrollLeft+iecompattest().clientWidth-15 : window.pageXOffset+window.innerWidth-15
dropmenuobj.contentmeasure=dropmenuobj.offsetWidth
if (windowedge-dropmenuobj.x < dropmenuobj.contentmeasure)
edgeoffset=dropmenuobj.contentmeasure-obj.offsetWidth
}
else{
var topedge=ie4 && !window.opera? iecompattest().scrollTop : window.pageYOffset
var windowedge=ie4 && !window.opera? iecompattest().scrollTop+iecompattest().clientHeight-15 : window.pageYOffset+window.innerHeight-18
dropmenuobj.contentmeasure=dropmenuobj.offsetHeight
if (windowedge-dropmenuobj.y < dropmenuobj.contentmeasure){
edgeoffset=dropmenuobj.contentmeasure+obj.offsetHeight
if ((dropmenuobj.y-topedge)<dropmenuobj.contentmeasure)
edgeoffset=dropmenuobj.y+obj.offsetHeight-topedge
}
}
return edgeoffset
}

function populatemenu(what){
if (ie4||ns6)
dropmenuobj.innerHTML=what.join("")
}


function dropdownmenu(obj, e, menucontents, menuwidth){
if (window.event) event.cancelBubble=true
else if (e.stopPropagation) e.stopPropagation()
clearhidemenu()
dropmenuobj=document.getElementById? document.getElementById("dropmenudiv") : dropmenudiv
populatemenu(menucontents)

if (ie4||ns6){
showhide(dropmenuobj.style, e, "visible", "hidden", menuwidth)
dropmenuobj.x=getposOffset(obj, "left")
dropmenuobj.y=getposOffset(obj, "top")
dropmenuobj.style.left=dropmenuobj.x-clearbrowseredge(obj, "rightedge")+"px"
dropmenuobj.style.top=dropmenuobj.y-clearbrowseredge(obj, "bottomedge")+obj.offsetHeight+"px"
}

return clickreturnvalue()
}

function clickreturnvalue(){
if (ie4||ns6) return false
else return true
}

function contains_ns6(a, b) {
while (b.parentNode)
if ((b = b.parentNode) == a)
return true;
return false;
}

function dynamichide(e){
if (ie4&&!dropmenuobj.contains(e.toElement))
delayhidemenu()
else if (ns6&&e.currentTarget!= e.relatedTarget&& !contains_ns6(e.currentTarget, e.relatedTarget))
delayhidemenu()
}

function hidemenu(e){
if (typeof dropmenuobj!="undefined"){
if (ie4||ns6)
dropmenuobj.style.visibility="hidden"
}
}

function delayhidemenu(){
if (ie4||ns6)
delayhide=setTimeout("hidemenu()",disappeardelay)
}

function clearhidemenu(){
if (typeof delayhide!="undefined")
clearTimeout(delayhide)
}

if (hidemenu_onclick=="yes")
document.onclick=hidemenu
