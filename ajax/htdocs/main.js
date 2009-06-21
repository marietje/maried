// (c) 2009 - Bas Westerbaan <bas@westerbaan.name>

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
	client.send(null);
}

function zeroPadLeft(s, n) {
	var pad = ''
	for(var i=0; i<n - s.length; i++)
		pad += '0';
	return pad + s;
}

function niceTime(tmp) {
	var neg = tmp < 0;
	if(neg) tmp = -tmp;
	tmp = parseInt(tmp)
	var secs = tmp % 60;
	tmp = parseInt(tmp /60);
	var mins = tmp % 60;
	tmp = parseInt(tmp / 60);
	var hrs = tmp;
	var ret;
	if(hrs == 0)
		ret = mins.toString() + ':' + 
		      zeroPadLeft(secs.toString(), 2);
	else
		ret = hrs.toString() + ':' +
		      zeroPadLeft(min.toString(), 2) + ':' +
		      zeroPadLeft(secs.toString(), 2);
	if(neg) ret = '-'+ret;
	return ret;
}

function Client() {
	this.run = function() {
		var client = this;
		this.got_playing = false;
		this.got_requests = false;
		this.got_media = false;
		this.updating_times = false;
		this.update_interval_id = null;
		this.update_requests = false;
		this.div_main = objById('main');
		this.fetching_playing = false;
		this.fetching_media = false;
		this.fetching_requests = false;
		this.create_query_field();
		this.create_requests_table();
		this.create_results_table();
		this.fetch_requests();
		this.fetch_media();
		this.fetch_playing();
		this.query = '';
		this.old_query = '';
		this.shift_down = false;
		this.ctrl_down = false;
		this.alt_down = false;
		this.qc = {};
		document.onkeydown = function() { client.on_keydown(event); }
		document.onkeyup = function() { client.on_keyup(event); }
	};
	this.on_keydown = function(event) {
		if (event.keyCode == 17) {
			this.ctrl_down = true;
		} else if (event.keyCode == 18) {
			this.alt_down = true;
		} else if(!this.ctrl_down &&
			  !this.alt_down) {
			if(event.keyCode >= 65 &&
		   	   event.keyCode <= 90 ||
			   event.keyCode == 32) {
				this.query += String.fromCharCode(
						event.keyCode).toLowerCase();
			} else if (event.keyCode == 8) {
				if(this.query.length != 0)
					this.query = this.query.slice(0,-1);
			}
		} else if (this.ctrl_down && !this.alt_down) {
			if(event.keyCode == 85) {
				this.query = '';
			}
		}
		this.do_updates()
	};
	this.on_keyup = function(event) {
		if(event.keyCode == 17) {
			this.ctrl_down = false;
		} else if (event.keyCode == 18) {
			this.alt_down = false;
		}
	};
	this.fetch_requests = function() {
		if(this.fetching_requests) return;
		this.fetching_requests = true;
		ajax_request('requests', function(doc) {
				client.on_got_requests(doc); });
	};
	this.fetch_playing = function() {
		if(this.fetching_playing) return;
		this.fetching_playing = true;
		ajax_request('playing', function(doc) {
				client.on_got_playing(doc); });
	};
	this.fetch_media = function() {
		if(this.fetching_media) return;
		this.fetching_media = true;
		ajax_request('media', function(doc) {
				client.on_got_media(doc); });
	};
	this.do_updates = function() {
		if(this.update_requests && this.got_requests) {
			this.empty_table(this.requests_table);
			this.fill_requests_table();
			this.update_requests = false;
		}
		if(this.got_playing && this.got_media !=
		   this.updating_times) {
			this.updating_times = !this.updating_times;
			if(this.updating_times) {
				var me = this;
				this.update_interval_id = setInterval(
					function() { me.update_times(); }, 1000);
			} else {
				clearInterval(this.update_interval_id);
			}
		}
		if(this.query != this.old_query) {
			if(this.query == '') {
				this.query_div.style.display = 'none';
				this.results_table.style.display = 'none';
				this.requests_table.style.display = 'block';
			} else {
				this.query_div.style.display = 'block';
				this.results_table.style.display = 'block';
				this.requests_table.style.display = 'none';
				this.query_div.firstChild.nodeValue = this.query;
				this.empty_table(this.results_table);
				this.fill_results_table();
			}
			this.old_query = this.query;
		}
	};
	this.update_times = function() {
		// we know got_playing and got_media
		var diff = (this.playing_endTime 
				- new Date().getTime() / 1000.0
				- this.playing_serverTime
				+ this.playing_requestTime);
		var els = this.requests_table.getElementsByTagName('tr');
		for(var i=0; i<els.length; i++) {
			var el = els[i].getElementsByClassName('time')[0];
			var txt = '';
			if(els[i].offsetTime != null)
				txt = niceTime(els[i].offsetTime + diff);
			el.firstChild.nodeValue = txt;
		}
		if(diff < 0) {
			this.fetch_requests();
			this.fetch_playing();
		}
	};
	this.on_got_requests = function(doc) {
		this.got_requests = true;
		this.fetching_requests = false;
		this.requests = [];
		var els = doc.firstChild.getElementsByTagName('request');
		for(var i=0; i<els.length; i++) {
			if(els[i].attributes['requestedBy'])
				var rb = els[i].attributes['requestedBy'].value;
			else var rb = 'marietje';
			this.requests[i] = {
				media: els[i].attributes['media'].value,
				requestedBy: rb };
		}
		this.update_requests = true;
		this.do_updates();
	};
	this.on_got_media = function(doc) {
		this.got_media = true;
		this.fetching_media = false;
		this.media = {};
		var els = doc.firstChild.getElementsByTagName('media');
		this.qc[''] = [];
		for(var i=0; i<els.length; i++) {
			this.media['_'+els[i].attributes['key'].value] = {
				length: parseFloat(els[i].attributes.getNamedItem('length').value),
				artist: els[i].attributes['artist'].value,
				title: els[i].attributes['title'].value
			};
			cleaned = els[i].attributes['artist'].value.toLowerCase().replace(/[^a-z0-9 ]/g,'') +
				"|" + els[i].attributes['title'].value.toLowerCase().replace(/[^a-z0-9]/g, '');
			this.qc[''][i] = [ els[i].attributes['key'].value, cleaned ];
		}
		this.update_requests = true;
		this.do_updates();
	};
	this.on_got_playing = function(doc) {
		this.got_playing = true;
		this.fetching_playing = false;
		this.playing_media = doc.firstChild.attributes['media'].value;
		this.playing_endTime = parseFloat(doc.firstChild.attributes['endTime'].value);
		this.playing_requestTime = new Date().getTime() / 1000.0;
		this.playing_serverTime = parseFloat(doc.firstChild.attributes['serverTime'].value);
		this.update_requests = true;
		this.do_updates();
	};
	this.create_results_table = function() {
		n_t = document.createElement('table');
		n_t.setAttribute('id', 'resultsTable');
		n_t.style.display = 'none';
		this.div_main.appendChild(n_t);
		this.results_table = n_t;
	};
	this.create_query_field = function() {
		n_d = document.createElement('div');
		n_d.setAttribute('id', 'query');
		n_d.appendChild(document.createTextNode('blaat'));
		n_d.style.display = 'none';
		this.div_main.appendChild(n_d);
		this.query_div = n_d;
	};
	this.create_requests_table = function() {
		n_t = document.createElement('table');
		n_t.setAttribute('id', 'requestsTable');
		this.div_main.appendChild(n_t);
		this.requests_table = n_t;
	};

	this.empty_table = function(table) {
		var trs = table.getElementsByTagName('tr');
		var toDel = [];
		for(var i=0; i<trs.length; i++)
			toDel[i] = trs[i];
		for(var i=0; i<toDel.length; i++)
			table.removeChild(toDel[i]);
	}

	this._create_tr_for_request = function(media, requestedBy, time) {
		n_tr = document.createElement('tr');
		if(this.got_media) {
			txta = this.media['_'+media].artist;
			txtt = this.media['_'+media].title;
		} else {
			txta = media;
			txtt = ''; 
		}
		n_td0 = document.createElement('td');
		n_td0.appendChild(document.createTextNode(
					requestedBy))
		n_td1 = document.createElement('td');
		n_td1.appendChild(document.createTextNode(txta));
		n_td2 = document.createElement('td');
		n_td2.appendChild(document.createTextNode(txtt));
		n_td3 = document.createElement('td');
		n_td3.setAttribute('class', 'time')
		n_td3.appendChild(document.createTextNode(time));
		n_tr.offsetTime = time;
		n_tr.appendChild(n_td0);
		n_tr.appendChild(n_td1);
		n_tr.appendChild(n_td2);
		n_tr.appendChild(n_td3);
		return n_tr;
	}

	this.do_query = function() {
		if(this.query == '')
			return;
		var s;
		for(s=this.query.length;
		    this.qc[this.query.slice(0,s)] == null;
		    s--);
		for(var i=s; i<this.query.length; i++){
			var from = this.query.slice(0,i);
			var to = this.query.slice(0,i+1);
			var k = 0;
			this.qc[to] = [];
			for(var j=0; j<this.qc[from].length; j++) {
				if(this.qc[from][j][1].indexOf(to) != -1) {
					this.qc[to][k] = this.qc[from][j];
					k += 1;
				}
			}
		}
	}

	this.fill_results_table = function() {
		if(!this.got_media)
			return;
		this.do_query();
		for(var i=0; i<this.qc[this.query].length; i++) {
			this.results_table.appendChild(
				this._create_tr_for_results(
					this.qc[this.query][i][0]));
			if(i == 40) break;
		}
	}

	this._create_tr_for_results = function(media) {
		n_tr = document.createElement('tr');
		n_tda = document.createElement('td');
		n_tdt = document.createElement('td');
		n_tda.appendChild(document.createTextNode(this.media['_'+media].artist));
		n_tdt.appendChild(document.createTextNode(this.media['_'+media].title));
		n_tr.appendChild(n_tda);
		n_tr.appendChild(n_tdt);
		return n_tr;
	}

	this.fill_requests_table = function() {
		var ctime = 0.0;
		if(this.got_playing) {
			this.requests_table.appendChild(
				this._create_tr_for_request(
					this.playing_media,
					'', null));
		}
							 
		for(var i=0; i<this.requests.length; i++){
			media = this.requests[i].media;
			requestedBy = this.requests[i].requestedBy;
			this.requests_table.appendChild(
				this._create_tr_for_request(media,
							    requestedBy,
							    ctime));
			if(this.got_media)
				ctime += this.media['_'+media].length;
		}
	};
}
