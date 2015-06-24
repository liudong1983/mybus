function decodeHTML(source){
	var str = String(source)
	.replace(/&quot;/g, '"')
	.replace(/&lt;/g, '<')
	.replace(/&gt;/g, '>')
	.replace(/&amp;/g, '&')
	.replace(/&#39;/g, "'");
	return str;
}
function encodeHTML(source){
	var str = String(source)
	.replace(/&/g, '&amp;')
	.replace(/</g, '&lt;')
	.replace(/>/g, '&gt;')
	.replace(/"/g, '&quot;')
	.replace(/'/g, '&#39;');
	return str;
}

