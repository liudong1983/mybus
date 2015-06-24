//时间和字符串互相转换

/*
  parseDate('2010-1-1') return new Date(2010,0,1)  
  4  parseDate(' 2010-1-1 ') return new Date(2010,0,1)  
  5  parseDate('2010-1-1 15:14:16') return new Date(2010,0,1,15,14,16)  
  6  parseDate(' 2010-1-1 15:14:16 ') return new Date(2010,0,1,15,14,16);  
  7  parseDate('2010-1-1 15:14:16.254') return new Date(2010,0,1,15,14,16,254)  
  8  parseDate(' 2010-1-1 15:14:16.254 ') return new Date(2010,0,1,15,14,16,254)  
  9  parseDate('不正确的格式') retrun null  
 10*/  
 function parseDate(str){   
   if(typeof str == 'string'){   
     var results = str.match(/^ *(\d{4})-(\d{1,2})-(\d{1,2}) *$/);
     if(results && results.length>3){
     return  new Date(parseInt(results[1],10),(parseInt(results[2],10) -1),parseInt(results[3],10));    
       }
     results = str.match(/^ *(\d{4})-(\d{1,2})-(\d{1,2}) +(\d{1,2}):(\d{1,2}):(\d{1,2}) *$/);   
     if(results && results.length>6)   
       return new Date(parseInt(results[1],10),parseInt(results[2],10) -1,parseInt(results[3],10),parseInt(results[4],10),parseInt(results[5],10),parseInt(results[6],10));    
     results = str.match(/^ *(\d{4})-(\d{1,2})-(\d{1,2}) +(\d{1,2}):(\d{1,2}):(\d{1,2})\.(\d{1,9}) *$/);   
     if(results && results.length>7)   
       return new Date(parseInt(results[1],10),parseInt(results[2],10) -1,parseInt(results[3],10),parseInt(results[4],10),parseInt(results[5],10),parseInt(results[6],10),parseInt(results[7],10));    
   }   
   return null;   
 }   
 
 
  /* 
    将Date/String类型,解析为String类型. 
    传入String类型,则先解析为Date类型 
    不正确的Date,返回 '' 
    如果时间部分为0,则忽略,只返回日期部分. 
  */  
  function formatDate(v){  
    if(typeof v == 'string') v = parseDate(v);  
    if(v instanceof Date){  
      var y = v.getFullYear();  
      var m = v.getMonth() + 1;  
      var d = v.getDate();  
      var h = v.getHours();  
      var i = v.getMinutes();  
      var s = v.getSeconds();  
      var ms = v.getMilliseconds();     
      if(ms>0) return y + '-' + m + '-' + d + ' ' + h + ':' + i + ':' + s + '.' + ms;  
      if(h>0 || i>0 || s>0) return y + '-' + m + '-' + d + ' ' + h + ':' + i + ':' + s;  
      return y + '-' + m + '-' + d;  
    }  
    return '';  
  }

// format the date as a string
Date.prototype.format = function(format) //author: meizz 
{ 
  var o = { 
    "y+" : this.getFullYear(), //year
    "M+" : this.getMonth()+1, //month 
    "d+" : this.getDate(),    //day 
    "h+" : this.getHours(),   //hour 
    "m+" : this.getMinutes(), //minute 
    "s+" : this.getSeconds(), //second 
    "q+" : Math.floor((this.getMonth()+3)/3),  //quarter 
    "S" : this.getMilliseconds() //millisecond 
  } 
  if(/(y+)/.test(format)) format=format.replace(RegExp.$1, 
    (this.getFullYear()+"").substr(4 - RegExp.$1.length)); 
  for(var k in o)if(new RegExp("("+ k +")").test(format)) 
    format = format.replace(RegExp.$1, 
      RegExp.$1.length==1 ? o[k] : 
        ("00"+ o[k]).substr((""+ o[k]).length)); 
  return format; 
} 
