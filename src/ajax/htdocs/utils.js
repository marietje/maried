// start from http://www.quirksmode.org/js/cookies.html
function createCookie(name,value,days) {
	if (days) {
		var date = new Date();
		date.setTime(date.getTime()+(days*24*60*60*1000));
		var expires = "; expires="+date.toGMTString();
	}
	else var expires = "";
	document.cookie = name+"="+value+expires+"; path=/";
}

function readCookie(name) {
	var nameEQ = name + "=";
	var ca = document.cookie.split(';');
	for(var i=0;i < ca.length;i++) {
		var c = ca[i];
		while (c.charAt(0)==' ') c = c.substring(1,c.length);
		if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length,c.length);
	}
	return null;
}

function eraseCookie(name) {
	createCookie(name,"",-1);
}
// end from http://www.quirksmode.org/js/cookies.html

function zpad_left(tmp, n) {
	var pad = ''
	for(var i = tmp.length; i < n; i++)
		pad += '0';
	return pad + tmp;
}

function nice_time(tmp) {
	neg = tmp < 0;
	if(neg) tmp = -tmp;
	var sec = parseInt(tmp % 60);
	tmp /= 60;
	var min = parseInt(tmp % 60);
	var hrs = parseInt(tmp / 60);
	if(hrs == 0)
		var ret = min.toString() + ':' + zpad_left(sec.toString(), 2);
	else
		var ret = hrs.toString() + ':' +
			  zpad_left(min.toString(), 2) + ':' +
			  zpad_left(sec.toString(), 2);
	if(neg) ret = '-' + ret;
	return ret;
}
