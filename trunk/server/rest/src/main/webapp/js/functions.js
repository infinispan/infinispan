function mgnlPopupWindow(strLink, width, height, menubar){
	var tmp = window.open(strLink , "popupWindow", "directories="+menubar+",location="+menubar+",toolbar="+menubar+",menubar="+menubar+",resizable=yes,scrollbars=yes,width="+width+",height="+height);
	tmp.focus();
}

function mgnlSwitchParagraphInformation(ID,icon,pageContext) {
	if (document.getElementById(ID).style.visibility == 'hidden') {

		document.getElementById(ID).style.visibility = "visible";
		document.getElementById(ID).style.display = "block";		
		document.getElementById(ID).style.height = "auto";
		if (icon == ID + "infoButtonLayoutBar") {
			document.getElementById(icon).src = pageContext + "/docroot/siteDesigner/pics/infoButtonRedActive.gif";
		} else {
			document.getElementById(icon).src = pageContext + "/docroot/siteDesigner/pics/infoButtonGreenActive.gif";
		}
		
	} else {

		document.getElementById(ID).style.visibility = "hidden";
		document.getElementById(ID).style.display = "none";
		document.getElementById(ID).style.height = "0px";
		if (icon == ID + "infoButtonLayoutBar") {
			document.getElementById(icon).src = pageContext + "/docroot/siteDesigner/pics/infoButtonRed.gif";
		} else {
			document.getElementById(icon).src = pageContext + "/docroot/siteDesigner/pics/infoButtonGreen.gif";
		}	
		
	}
}

function blurAnchors() {
	if (document.getElementsByTagName) {
		var a = document.getElementsByTagName("a");
		for(var i = 0; i < a.length; i++) {
			a[i].onfocus = function() {
				this.blur()
			};
		};
	};
};