//截断小数位数，返回值为字符串
function cutFixedNum(value,num){
	var value_str = "" + value;
	var index = value_str.indexOf('.');
	if (index!=-1){
		value_str = value_str.substring(0,index+num+1);
	}
	return value_str;
}
//string trim
String.prototype.trim = function() 
{ 
	return this.replace(/(^\s*)|(\s*$)/g, ""); 
} 
String.prototype.ltrim = function() 
{ 
	return this.replace(/(^\s*)/g, ""); 
} 
String.prototype.rtrim = function() 
{ 
	return this.replace(/(\s*$)/g, ""); 
} 
//格式化字符串 用法
/*
var template = "{0}，欢迎你在'{1}'上给{2}留言，交流看法";
var msg = String.format(template, "你好", "深海博客","Torry");
alert(msg);
*/
String.format = function(src){
    if (arguments.length == 0) return null;
    var args = Array.prototype.slice.call(arguments, 1);
    return src.replace(/\{(\d+)\}/g, function(m, i){
        return args[i];
    });
};

