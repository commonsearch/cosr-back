$(function() {

	/*
		Quick script to manage the Explainer frontend. Could be improved in many ways!
	*/

	$.urlParam = function(name){
	    var results = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.href);
	    if (results==null){
	       return null;
	    }
	    else{
	       return decodeURIComponent(results[1]).replace(/\+/g, " ") || 0;
	    }
	}

	var urldebug = function(url) {
		$("#url").val(url);

		$.get("/api/urldebug", {"url": url}, function(data) {
			data = JSON.parse(data);
			console.log(data);

			var tpl = $("#tpl_urldebug").html();

			var html = _.template(tpl)({"data": data});

			$("#urldebug_content").html(html);

		});

	};

	var searchdebug = function(query, lang) {
		$("#q").val(query);
		$("#g").val(lang);

		$.get("/api/searchdebug", {"q": query, "g": lang}, function(data) {
			data = JSON.parse(data);
			console.log(data);

			var tpl = $("#tpl_searchdebug").html();

			var html = _.template(tpl)({"data": data});

			$("#searchdebug_content").html(html);

		});

	};

	// TODO HTML5 history to avoid the submit request
	// $("#urlform").on("submit", function(evt) {
	// 	var url = $("#url").val();
	// 	urldebug(url);
	// 	return false;
	// });

	var initUrl = $.urlParam("url");
	if (initUrl) {
		urldebug(initUrl);
	}

	var initSearch = $.urlParam("q");
	if (initSearch) {
		searchdebug($.urlParam("q"), $.urlParam("g") || "en");
	}


});