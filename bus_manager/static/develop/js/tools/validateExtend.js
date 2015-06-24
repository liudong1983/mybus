// ip list  例: 10.2.161.15,10.2.161.17
jQuery.validator.addMethod("iplist", function(value, element) {       
return this.optional(element) || /^(((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)(\.((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)){3}\,)*((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)(\.((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)){3}$/.test(value);  
}, "Please enter a valid IP list");    
// ip  例: 10.2.161.15
jQuery.validator.addMethod("ip", function(value, element) {       
return this.optional(element) || /^((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)(\.((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)){3}$/.test(value);  
}, "Please enter a valid IP");  

// 增加只能是字母、数字、下划线的验证         
jQuery.validator.addMethod("chrnum", function(value, element) {         
   return this.optional(element) || (/^([a-zA-Z0-9_]+)$/.test(value));         
}, "只能输入数字、字母、下划线或者它们的组合");

//emaillist  例：**@autonavi.vom, **@126.com
jQuery.validator.addMethod( "emaillist", function(value, element) {        
	return this.optional(element) || /^(((([a-z]|\d|[!#\$%&'\*\+\-\/=\?\^_`{\|}~]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])+(\.([a-z]|\d|[!#\$%&'\*\+\-\/=\?\^_`{\|}~]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])+)*)|((\x22)((((\x20|\x09)*(\x0d\x0a))?(\x20|\x09)+)?(([\x01-\x08\x0b\x0c\x0e-\x1f\x7f]|\x21|[\x23-\x5b]|[\x5d-\x7e]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])|(\\([\x01-\x09\x0b\x0c\x0d-\x7f]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF]))))*(((\x20|\x09)*(\x0d\x0a))?(\x20|\x09)+)?(\x22)))@((([a-z]|\d|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])|(([a-z]|\d|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])([a-z]|\d|-|\.|_|~|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])*([a-z]|\d|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])))\.)+(([a-z]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])|(([a-z]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])([a-z]|\d|-|\.|_|~|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])*([a-z]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])))\,)*((([a-z]|\d|[!#\$%&'\*\+\-\/=\?\^_`{\|}~]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])+(\.([a-z]|\d|[!#\$%&'\*\+\-\/=\?\^_`{\|}~]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])+)*)|((\x22)((((\x20|\x09)*(\x0d\x0a))?(\x20|\x09)+)?(([\x01-\x08\x0b\x0c\x0e-\x1f\x7f]|\x21|[\x23-\x5b]|[\x5d-\x7e]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])|(\\([\x01-\x09\x0b\x0c\x0d-\x7f]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF]))))*(((\x20|\x09)*(\x0d\x0a))?(\x20|\x09)+)?(\x22)))@((([a-z]|\d|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])|(([a-z]|\d|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])([a-z]|\d|-|\.|_|~|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])*([a-z]|\d|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])))\.)+(([a-z]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])|(([a-z]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])([a-z]|\d|-|\.|_|~|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])*([a-z]|[\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])))$/i.test(value);
	}, "只能输入邮件或者邮件列表" );

// mobile list  例: 130********,159********
jQuery.validator.addMethod("mobilelist", function(value, element) {       
	return this.optional(element) || /^([0-9 \(\)]{7,30}\,)*[0-9 \(\)]{7,30}$/.test(value);
	}, "请输入手机号或者手机号列表");	

// 扩展 validator，validator 本身提供了url 
// 形如 http://{ip}:{port}/apple?a=ty&b=15
// 或 http://www.autonavi.com/apple?a=ty&b=15&ip={ip}&port={port}
jQuery.validator.addMethod("httpurl", function(value, element) {       
	return this.optional(element) || /^http:\/\/[\w\-\{\}]+(\.[\w\-\{\}]+)*([\w\-\{\}\.,@?^=%&amp;:/~\+#]*[\w\-\{\}\@?^=%&amp;/~\+#])?$/.test(value);
}, "请输入合法的HTTP请求URL");	


//长度不超过300       
jQuery.validator.addMethod("content_1000", function(value, element) {
   return this.optional(element) || (/^[\s\S]{0,1000}$/.test(value));         
}, "请在1000个字符内描述内容");


// 增加对数字的验证
jQuery.validator.addMethod("checknum", function(value, element) {
   return this.optional(element) || (/^([0-9]+)$/.test(value));         
}, "只能输入数字");


// 多个IP，每行写一个IP
jQuery.validator.addMethod("checkiplist", function(value, element) {
return this.optional(element) || /^(((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)(\.((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)){3}\s)*((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)(\.((25[0-5])|(2[0-4]\d)|(1\d\d)|([1-9]\d)|\d)){3}$/.test(value);  
}, "请输入有效的IP列表");





