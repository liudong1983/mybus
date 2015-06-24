// 存储hash值和url的对应关系
hash_dict = {}

function success(data){
	//$('#content').fadeOut();
	$("#content").html(data);
	//$('#content').fadeIn();
	docReady();
}
function error(data){
	alert('something is error!');	
}
//用来请求网页显示在固定位置
//　flag: 是否时左侧的主菜单
function executeMenu(element,flag){
    var url = $(element).attr("href");

    if(flag){
	$('ul.main-menu li.active').removeClass('active');
	$(element).parent('li').addClass('active');	
	// 记录历史URL 便于回退
	$("#hide_history_url").val(url);
	
	var hash_value  = '#' + parseInt(Math.random()*100000);
	location.hash = hash_value;
	hash_dict[hash_value] = $("#hide_history_url").val();
	
	// 改变title
	//var base = $($("meta[name='base_title']")[0]).attr('content');
	//$("title").html(base + '-' + $($(element).children()[1]).html().trim());
    }
	

    jQuery.ajax({
        type: 'GET',
        url: url,
        success: success,
        error:error,
        dataType: 'html',
        async:false
    }); 
    return false;
}

//处理刷新页面，json字符串包含statusCode，url，message信息
function httpRedirect(data){
	var statusCode = data.statusCode;
	var url = data.url;
	var message = data.message;
	if (statusCode == 200){
		httpRedirectAjax(url);
	}
	alert(message);
}

//执行删除或批量删除
function executeDelete(obj,ids){
	var url = $(obj).attr("href");
	var message = $(obj).attr("title");
	if (!confirm(message))
  	{
  		return false;
  	}
	jQuery.ajax({
		type: 'POST',
		url: url,
		data:{'ids':ids},
		success:httpRedirect,
		error: error,
		dataType: 'json',
		async:false
	});	
	return false;
}
//请求url，刷新请求页面
function httpRedirectAjax(url){
    jQuery.ajax({
		type: 'GET',
		url: url,
		success: success,
		error:error,
		dataType: 'html',
		async:false
	});	
	return false;
}


// 回退到历史URL
function backHistoryURL(){
	jQuery.ajax({
		type: 'GET',
		url: $("#hide_history_url").val(),
		success: success,
		error:error,
		dataType: 'html',
		async:false
	});	
	return false;
}
//处理form表单,返回一个json字符串给httpRedirect函数进行重定向
function validateCallback(form){
	//校验失败，直接返回
	if(!$(form).valid()){
		alert("表单校验失败，无法提交!");
		return false;
	}
	var url = $(form).attr("action");
	jQuery.ajax({
		type: 'POST',
		url: url,
		data:$(form).serializeArray(),
		success: httpRedirect,
		error:error,
		dataType: 'json',
		async:false
	});	
	return false;
}

// 上一页，下一页
function pageJump(obj){
	//当前页,总页数	
	var url = $(obj).attr("href");
	var t = $(obj).parent().parent();
	var currPage = parseInt(t.attr("currPage"));
	var totalPage = parseInt(t.attr("totalPage"));
	var pageNum = 1;
	switch($(obj).parent().attr("class")){
		case "prev":
			if(currPage > 1){
				pageNum = currPage - 1;
			}else{
				pageNum = 1;
			}
			break;
		case "next":
			if(currPage < totalPage){
				pageNum = currPage + 1;
			}else{
				pageNum = totalPage;
			}
			break;
	}
	jQuery.ajax({
		type: 'POST',
		url: url,
		data:{'pageNum':pageNum},
		success:success,
		error: error,
		dataType: 'html',
		async:false
	});	
	return false;
}



//删除某个已选定的图表
function remove_item(obj){  
	$(obj).parent().parent().remove();
}

// ------------------- 用于搜索  ---------------------
function search_success(data){
	//$('#content').fadeOut();
	$("#box-content").children("table").remove();
	$("#box-content").children(".row-fluid").remove();
	$("#box-content").append(data);
	//$('#content').fadeIn();
}

//  搜索查询操作
function searchCallback(){

    //校验失败，直接返回
/*
    if(!$("form").valid()){
	alert("表单校验失败，无法提交!");
	return false;
    }
*/
    var url = $("#search_form").attr("action");
    jQuery.ajax({
	type: 'POST',
	url: url,
	data:$("form").serializeArray(),
	success: search_success,
	error:error,
	dataType:'html',
	async:false
     });
     return false;
}
//搜索结果页中的页面跳转,首页，末页，前一页，后一页，确定按钮
function searchPageJump(obj){
	//当前页,总页数	
	var url = $(obj).attr("href");
	var t = $(obj).parent().parent();
	var currPage = parseInt(t.attr("currPage"));
	var totalPage = parseInt(t.attr("totalPage"));
	var pageNum = 1;
	switch($(obj).parent().attr("class")){
		case "prev":
			if(currPage > 1){
				pageNum = currPage - 1;
			}else{
				pageNum = 1;
			}
			break;
		case "next":
			if(currPage < totalPage){
				pageNum = currPage + 1;
			}else{
				pageNum = totalPage;
			}
			break;
	}
	var param_data = $("form").serializeArray();
	param_data.push({'name':'pageNum','value':pageNum});
	jQuery.ajax({
		type: 'POST',
		url: url,
		data:param_data,
		success:search_success,
		error: error,
		dataType: 'html',
		async:false
	});	
	return false;
}

function check_name(){
	var url = "/dynamicconfig/search";
	var keyword = $("#accountname").val();
	var data = {'keyword':keyword,'exactSearch':'exactSearch'};
        jQuery.ajax({
		type: 'GET',
		url: url,
		data: data,
		success: check_success,
		error:error,
		dataType: 'json',
		async:false
	});
}
function check_success(data){
	// 正确的
	if(data&&data.length==1){
		$("#check_name").val('1');
	}else{
		$("#check_name").val('0');
	}
}
//处理form表单,返回一个json字符串给httpRedirect函数进行重定向
function validateCallbackOrder(form){
	//校验失败，直接返回
	if(!$(form).valid()){
		alert("表单校验失败，无法提交!");
		return false;
	}
	
	/*
	// 检查申报人是否合法
	check_name();

	if($("#check_name").val()=='0'){
		alert('申报人不合法');
		$("#realname").val('');
		$("#realname")[0].focus();
		return false;
	}
	*/
	
	var url = $(form).attr("action");
	jQuery.ajax({
		type: 'POST',
		url: url,
		data:$(form).serializeArray(),
		success: httpRedirect,
		error:error,
		dataType: 'json',
		async:false
	});	
	return false;
}


// hash变更触发此事件
function hashChangeFire(){
	//alert(hash_dict[location.hash]);
	httpRedirectAjax(hash_dict[location.hash]);
}


// 切换Tab项的div
var cur_tabid = 'tab1'; // 默认为第一项
function switchTab(tabid) {
	cur_tabid = tabid;
	return true;
}


// ------------------- 用于状态检查  ---------------------
function check_success(data){
	$("#" + cur_tabid).children("table").remove();
	$("#" + cur_tabid).children(".row-fluid").remove();
	//$("#" + cur_tabid).append(data);
	resultId = cur_tabid + "result"
	$("#" + resultId).remove();
	$("#" + cur_tabid).append("<div id=" + resultId + "></div>");
	$("#" + resultId).html(data);
}

// 用于服务器状态检查
function serverCheck(url, formid) {
    jQuery.ajax({
	type: 'POST',
	url: url,
	data:$("#" + formid).serializeArray(),
	success: check_success,
	error:error,
	dataType:'html',
	async:false
     });
     return false;
}


// ------------------- 用于服务相关操作  ---------------------
function service_success(data){
	$("#" + cur_tabid).children("table").remove();
	$("#" + cur_tabid).children(".row-fluid").remove();
	//$("#" + cur_tabid).append(data);
	resultId = cur_tabid + "result"
	$("#" + resultId).remove();
	$("#" + cur_tabid).append("<div id=" + resultId + "></div>");
	$("#" + resultId).html(data);
}

// 服务相关操作
function serviceCallback(formid) {
	//校验失败，直接返回
	if(!$("#" + formid).valid()){
		alert("表单校验失败，无法提交!");
		return false;
	}
	
    var url = $("#" + formid).attr("action");
    jQuery.ajax({
	type: 'POST',
	url: url,
	data:$("#" + formid).serializeArray(),
	success: service_success,
	error:error,
	dataType:'html',
	async:false
     });
     return false;
}


