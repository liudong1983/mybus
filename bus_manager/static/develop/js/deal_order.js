//提交备注
function submit_comment(){
	var comment = $("#comment").val();
	comment = comment.trim();
	// 备注不能为空
	if(!comment){
		alert('备注不应该为空.');
		return;
	}
	//--------- 前端操作 ---------
	// 用户
	var realname = $("#realname").val();
	// 时间
	var time = new Date().format("yyyy-MM-dd hh:mm");

	var item = $("<div></div>");
	var t = $("<h6></h6>");
	t.html(realname + " " + time);
	item.append(t);
	t = $('<div style="width:600px;margin-bottom:10px"></div>');
	t.html(comment);
	item.append(t);
	
	$("#comment").remove();
	$("#comment_div").append(item);
	$("#comment_div").append('<textarea class="textarea_std" name="comment" id="comment"></textarea>');
	
	//--------- ajax操作 --------
	//工单ID
	var order_id = $("#order_id").val();
	
	var url = "/order/submit_comment/";
	var data = {'order_id':order_id,'comment':comment};
        jQuery.ajax({
		type: 'POST',
		url: url,
		data: data,
		success:submit_comment_success,
		error:error,
		dataType: 'json',
		async:false
	});
	return false;
}
function submit_comment_success(){
	alert("备注提交成功!");
}
// 问题升级
function problem_upgrade(){
	$('#wander_div').modal({show:true,backdrop:'static'});
	return false;
}

// 问题升级面板中的确认按钮　
function wander_confirm(){
	$('#wander_div').modal('hide');

	var comment = $("#comment").val();
	var order_id = $("#order_id").val();
	var staff_id = $("#first_engineer_id").val();
	var url = "/order/wander/";
	var data = {'order_id':order_id,'staff_id':staff_id,'comment':comment};
        jQuery.ajax({
		type: 'POST',
		url: url,
		data: data,
		success:httpRedirect,
		error:error,
		async:false
	});
}
