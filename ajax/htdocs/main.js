if( typeof XMLHttpRequest == "undefined" ) XMLHttpRequest = function() {
	try { return new ActiveXObject("Msxml2.XMLHTTP.6.0") } catch(e) {}
	try { return new ActiveXObject("Msxml2.XMLHTTP.3.0") } catch(e) {}
	try { return new ActiveXObject("Msxml2.XMLHTTP") } catch(e) {}
	try { return new ActiveXObject("Microsoft.XMLHTTP") } catch(e) {}
	throw new Error( "This browser does not support XMLHttpRequest." )
};

function objById(id) {
	var ret = 0;
	if (document.getElementById)
		ret = document.getElementById(id);
	else if (document.all)
		ret = document.all[id];
	else if (document.layers)
		ret = document.layers[id];
	return ret;
}

function _ajax_request_orsc(req, callback) {
	if(req.readyState == 4) {
		callback(req.responseXML);
	}
}

function ajax_request(path, callback) {
	var client = new XMLHttpRequest();
	client.onreadystatechange = function () { 
		_ajax_request_orsc(this, callback);
	};
	client.open("GET", '/' + path);
	client.send();
}

function Client() {
	this.run = function() {
		var client = this;
		this.got_playing = false;
		this.got_requests = false;
		this.got_media = false;
		this.got_requests_table = false;
		this.div_main = objById('main');
		ajax_request('requests', function(doc) {
				client.on_got_requests(doc); });
		ajax_request('media', function(doc) {
				client.on_got_media(doc); });
		ajax_request('playing', function(doc) {
				client.on_got_playing(doc); });
	};
	this.on_got_requests = function(doc) {
		this.got_requests = true;
		this.requests = [];
		var els = doc.firstChild.getElementsByTagName('request');
		for(var i=0; i<els.length; i++) {
			if(els[i].attributes['requestedBy'])
				var rb = els[i].attributes['requestedBy'].value;
			else var rb = null;
			this.requests[i] = {
				media: els[i].attributes['media'].value,
				requestedBy: rb };
		}
		if(!this.got_requests_table)
			this.create_requests_table();
		else
			this.empty_requests_table();
		this.fill_requests_table();
	};
	this.on_got_media = function(doc) {
		this.got_media = true;
		this.media = {};
		var els = doc.firstChild.getElementsByTagName('media');
		for(var i=0; i<els.length; i++) {
			this.media['_'+els[i].attributes['key'].value] = {
				key: els[i].attributes['key'].value,
				length: els[i].attributes['length'].value,
				artist: els[i].attributes['artist'].value,
				title: els[i].attributes['title'].value
			};
		}
	};
	this.on_got_playing = function(doc) {
	};
	this.create_requests_table = function() {
		n_t = document.createElement('table');
		this.div_main.appendChild(n_t);
		this.requests_table = n_t;
	};
	this.empty_requests_table = function() {
		var trs = this.requests_table.getElementsByTagName('tr');
		for(var i=0; i<trs.length; i++) {
			this.requests_table.removeChild(trs[i]);
		}
	};
	this.fill_requests_table = function() {
		for(var i=0; i<this.requests.length; i++){
			n_tr = document.createElement('tr');
			this.requests_table.appendChild(n_tr);
			if(this.got_media) {
				txta = this.media[this.requests[i].media].artist;
				txtt = this.media[this.requests[i].media].title;
			} else {
				txta = this.requests[i].media;
				txtt = ''; 
			}
			n_td1 = document.createElement('td');
			n_td1.appendChild(document.createTextNode(txta));
			n_td2 = document.createElement('td');
			n_td2.appendChild(document.createTextNode(txtt));
			n_td3 = document.createElement('td');
			n_td3.appendChild(document.createTextNode(
						this.requests[i].requestedBy))
			n_tr.appendChild(n_td1);
			n_tr.appendChild(n_td2);
			n_tr.appendChild(n_td3);
		}
	};
}
