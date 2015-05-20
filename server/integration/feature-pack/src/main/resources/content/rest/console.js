/*
 ~ Copyright 2012 Red Hat, Inc. and/or its affiliates.
 ~
 ~ This is free software; you can redistribute it and/or modify it
 ~ under the terms of the GNU Lesser General Public License as
 ~ published by the Free Software Foundation; either version 2.1 of
 ~ the License, or (at your option) any later version.
 ~
 ~ This software is distributed in the hope that it will be useful,
 ~ but WITHOUT ANY WARRANTY; without even the implied warranty of
 ~ MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 ~ Lesser General Public License for more details.
 ~
 ~ You should have received a copy of the GNU Lesser General Public
 ~ License along with this library; if not, write to the Free Software
 ~ Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 ~ 02110-1301 USA
*/

var userValue = function(line) { return line ? line.substring(line.search( /(\s\b)/ )) : line; }

var getAjaxArgs = function( commandArgs ) {
	var headers = { performAsync: $("#perform-async").val() };
	if ($("#ttl").val()) { headers.timeToLiveSeconds = $("#ttl").val(); }
	if ($("#max-idle").val()) { headers.maxIdleTimeSeconds = $("#max-idle").val(); }
	
	return jQuery.extend( true, { 
		url: window.location.origin + commandArgs.url, success:onCmdSuccess, error:onCmdError, context: ($('#history')[0]), cache:false, headers: headers
	}, commandArgs);
}

var commands = {
		POST:	{
			execute: function(args) { var ajaxArgs = getAjaxArgs({ type:'POST',url:args.url,data:args.value });
				$.ajax( ajaxArgs );
			},
			usage : "POST /rest/{cacheName}/{cacheKey} {value}",
			help : 	"place the given value in the given cache with the specified key.<br/>If the data exists the content will not be updated and a CONFLICT will be returned",
		},
		PUT:	{
			execute: function(args) { var ajaxArgs = getAjaxArgs({ type:'PUT',url:args.url,data:args.value });
				$.ajax( ajaxArgs );
			},
			usage : "PUT /rest/{cacheName}/{cacheKey} {value}",
			help : 	"place the given value in the given cache with the specified key.<br/>Existing data will be replaced",
		},
		GET: {
			execute: function(args) { args.url = args.url || "/rest/default/"; var ajaxArgs = getAjaxArgs({ type:'GET',url:args.url});
				$.ajax( ajaxArgs );
			},
			usage : "GET /rest/{cacheName}/{cacheKey}",
			help : 	"return the data found under the given key in the given cache"
		},
		HEAD:	{
			execute: function(args) { var ajaxArgs = getAjaxArgs({ type:'HEAD', url:args.url});
				$.ajax( ajaxArgs );
			},
			usage :	"HEAD /rest/{cacheName}/{cacheKey} the same as GET, only no content is returned",
			help :	"similar to GET only headers are returned, not content."

		},
		DELETE:	{
			execute: function(args) { var ajaxArgs = getAjaxArgs({ type:'DELETE', url:args.url});
				$.ajax( ajaxArgs );
			},
			usage : "DELETE /rest/{cacheName} to remove everything <br/>DELETE /rest/{cacheName}/{cacheKey}",
			help :	"Remove the given key/value from the given cache or clear the cache when no key is specified"
		},
		HELP: {
			execute : function( ) {
				$.each(commands, function(key, value) { 
						$('#history').append(LI.replace(/X/,value.usage + "<br/>" + value.help ));
				});
				var console = ($('#history')[0]);
				console.scrollTop=console.scrollHeight;

			},
			usage : "HELP",
			help : "Print this message."
		}
}

var onCmdSuccess = function(data, textStatus, jqXHR) {
	var output = (  $('#history > li:last > div') );
	$(output).addClass("success");
	data = (data && data.length > 0) ? data.replace(">", "&gt;").replace("<", "&lt;") : "success"
	$(output).append( "<pre class='result'>[" + jqXHR.status + "] " + ( (data && data.length > 0) ? data : "success") + "</pre>");
	this.scrollTop=this.scrollHeight;
	return false;
}

var onCmdError = function(jqXHR, textStatus, errorThrown) {
	var output = (  $('#history > li:last > div') );
	$(output).addClass("failure");
	$(output).append( "<pre class='result'>[" + jqXHR.status + "] " + errorThrown + "</pre>");
	this.scrollTop=this.scrollHeight;
	return false;
}

var LI = "<li class='cmd'><div><pre class='input'>&gt;  X </pre></div></li>";

var trimCommand = function(userArgs) {
	userArgs = userArgs || "";
	var idx = userArgs.search(/\s/);
	return {
		cmd: userArgs.slice(0,idx).toUpperCase(),
		args: userArgs.slice(idx +1)
	}
}

var trimURL = function( args) {
	args = args || "";
	var idx = args.search(/\s/);
	return {
		url : (idx == -1 ? args : args.slice(0,idx)),
		value : args.slice(idx+1)
	}
}

var onCmdSubmit = function(evt) {
	$('#history').append(LI.replace(/X/, $(this).val()));
	
	try {		
		var input = trimCommand( $(this).val() );
		(commands[ input.cmd ] || commands.HELP).execute(trimURL(input.args));		
	} catch (e) { if (console) console.log(e); } 

	$(this).val('');
	return false;
}

var HistoryInput = function(element, onsubmit) {
	this.history = [];
	this.position = 0;
	this.input = $(element);
	this.input.keydown($.proxy(this.keyHandler, this));
	this.onsubmit = $.proxy(onsubmit, this.input);
}

HistoryInput.prototype.keyHandler = function(event) {
	switch (event.keyCode) {
	case 13:
		this.history.push($(this.input).val());
		this.position = this.history.length;
		this.onsubmit(event);
		break;
	case 38:
		this.position--;
		if (this.position >= 0) {
			$(this.input).val(this.history[this.position]);
		} else {
			this.position = 0;
		}
		event.preventDefault();
		break;
	case 40:
		this.position++;
		if (this.position < this.history.length) {
			$(this.input).val(this.history[this.position]);
		} else {
			this.position = this.history.length;
			$(this.input).val("");
		}
		event.preventDefault();
		break;
	default:
		break;
	}
}

$(document).ready(function() {
	 new HistoryInput($('#prompt'), onCmdSubmit);
	 
	 $('#console-wrapper a').click( function() {
		 $('body').toggleClass('visible-console');
		return false; 
	 });
	 
	 $('.try-it').click( function() {
		$('body').addClass('visible-console');
		return false;
	 });
	 
	 $('#cmd').ajaxStop(function() {
		this.disabled = false;
	 });
	 
	 $('#cmd').ajaxStart(function() {
		this.disabled = true;
	 });
});




