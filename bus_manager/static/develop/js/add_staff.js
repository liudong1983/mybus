function staff_success(data){
	$("#result_div").html("");
	if(data&&data.length>0){

		var i =0;
		for(i=0;i<data.length;i++){
			var t = $("<div></div>");
			t.addClass("cn_mail alert alert-success");
			t.attr("title","双击选定");
			t.attr("realname", data[i].cn);
			t.attr("accountname", data[i].sAMAccountName);
			t.attr("department", data[i].department);
			t.attr("email",data[i].mail);
			t.attr("phone",data[i].phone);
			t.html(data[i].cn + '[' + data[i].sAMAccountName + ']');
			$("#result_div").append(t);
		}
	}else{  
		$("#result_div").html("没有满足条件的结果.");
	}
	// 双击添加到收件人列表
	//　必须每次执行完搜索后都要执行，否则相应的处理动作，没有注册
	$(".cn_mail").dblclick(function(){
		$("#realname").val($(this).attr("realname"));
		$("#accountname").val($(this).attr("accountname"));
		$("#department").val($(this).attr("department"));
		$("#email").val($(this).attr("email"));
		$("#phone").val($(this).attr("phone"));
		close_staff_div();
	});
}
function staff_success_recommend(data){
	$("#result_div").html("");
	if(data&&data.length>0){

		var i =0;
		for(i=0;i<data.length;i++){
			var t = $("<div></div>");
			t.addClass("cn_mail alert alert-success");
			t.attr("title","双击选定");
			t.attr("realname", data[i].cn);
			t.attr("accountname", data[i].sAMAccountName);
			t.attr("department", data[i].department);
			t.attr("email",data[i].mail);
			t.attr("phone",data[i].phone);
			t.html(data[i].cn + '[' + data[i].sAMAccountName + ']');
			$("#result_div").append(t);
		}
	}else{  
		$("#result_div").html("没有满足条件的结果.");
	}
	// 双击添加到收件人列表
	//　必须每次执行完搜索后都要执行，否则相应的处理动作，没有注册
	$(".cn_mail").dblclick(function(){
		$("#realname").val($(this).attr("realname"));
		$("#accountname").val($(this).attr("accountname"));
		$("#department").val($(this).attr("department"));
		$("#email").val($(this).attr("email"));
		$("#phone").val($(this).attr("phone"));
		close_staff_div();
		recommend_engineer();
	});
}

function close_staff_div(){
	$("#keyword").val("");
	$("#result_div").html("");
	$('#staff_div').modal('hide');
}
function staff_error(){
	$("#result_div").html("搜索失败或搜索超时.");	
}
function staff_search(){
	var url = "/dynamicconfig/search";
	keyword = $("#keyword").val();
	keyword = keyword.trim();
	var data = {'keyword':$("#keyword").val()}
        jQuery.ajax({
		type: 'GET',
		url: url,
		data: data,
		success: staff_success,
		error:staff_error,
		dataType: 'json',
		async:false
	});
}
// 第一工程师区域选择
function area_staff_success_first(data){
	$("#first_engineer_id").html("");
	if(data.length>0){
		var i =0;
		for(i=0;i<data.length;i++){
			var t = $("<option></option>");
			t.html(data[i].realname);
			t.val(data[i].id);
			$("#first_engineer_id").append(t);
			
		}
	}
}

// 第二工程师区域选择
function area_staff_success_second(data){
	$("#second_engineer_id").html("");
	if(data.length>0){
		var i =0;
		for(i=0;i<data.length;i++){
			var t = $("<option></option>");
			t.html(data[i].realname);
			t.val(data[i].id);
			$("#second_engineer_id").append(t);
			
		}
	}
}
// 装载二级问题类目
function second_level_load(data){
	$("#second_level_problem").html("");
	if(data.length>0){
		var i =0;
		for(i=0;i<data.length;i++){
			var t = $("<option></option>");
			t.html(data[i].text);
			t.val(data[i].id);
			$("#second_level_problem").append(t);
			
		}
	}
}
// 装载三级问题类目
function third_level_load(data){
	$("#third_level_problem").html("");
	if(data.length>0){
		var i =0;
		for(i=0;i<data.length;i++){
			var t = $("<option></option>");
			t.html(data[i].text);
			t.val(data[i].id);
			$("#third_level_problem").append(t);
			
		}
	}
}
// 当用户名输入框失去焦点
function realname_blur(){
	var realname = $("#realname").val();
	var url = "/dynamicconfig/search";
	var data = {'keyword':realname};
        jQuery.ajax({
		type: 'GET',
		url: url,
		data: data,
		success: auto_search_staff,
		error:staff_error,
		dataType: 'json',
		async:false
	});
}

function auto_search_staff(data){
	//　只有一项
	if(data&&data.length==1){

		$("#realname").val(data[0].cn);
		$("#accountname").val(data[0].sAMAccountName);
		$("#department").val(data[0].department);
		$("#email").val(data[0].mail);
		$("#phone").val(data[0].phone);

	//多项或者没有查到数据
	}else{
		$('#staff_div').modal({show:true,backdrop:'static'});
		$("#accountname").val('');
		staff_success_recommend(data);
	}
}
//推荐工程师
function recommend_engineer(){
 	var area_id = parseInt($("#area_id").val());
 	var accountname = $("#accountname").val();
	var url = "/staff/recommend_engineer";
	var data = {'area_id':area_id, 'accountname':accountname};
	jQuery.ajax({
		type: 'GET',
		url: url,
		data: data,
		success: recommend_success,
		error: error,
		dataType: 'json',
		async:false
	});
}

//推荐工程师
function recommend_success(data){
	//　推荐
	$("#recommend_engineer").html(data.comment);
	if(data.area_id){
		$("#area_id_first").val(data.area_id);
		$("#area_id_first").change();
	}
	if(data.id){
		$("#first_engineer_id").val(data.id);
	}
	if(data.priority_level){
		$("#priority_level").val(data.priority_level);
	}
}
