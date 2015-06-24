function resourcepool_success(data){
	$("#resourcepool_result_div").html("");
	if(data&&data.length>0){
		var i =0;
		for(i=0;i<data.length;i++){
			var t = $("<div></div>");
			t.addClass("sequence_ip alert alert-success");
			t.attr("title","双击选定");
			t.attr("nocid", data[i].nocid);
			t.attr("nocabbr", data[i].nocabbr);
			t.attr("nocname", data[i].nocname);
			t.attr("elementid",data[i].elementid);
			t.attr("sequence",data[i].sequence);
			t.attr("equipmentmodel",data[i].equipmentmodel);
			t.attr("manage_ip",data[i].manage_ip);
			t.attr("in_ip",data[i].in_ip);
			t.attr("cpu",data[i].cpu);
			t.attr("memory",data[i].memory);
			t.attr("disk",data[i].disk);
			t.html(data[i].sequence + '[' + data[i].in_ip + ']');
			$("#resourcepool_result_div").append(t);
		}
	}else{  
		$("#resourcepool_result_div").html("没有满足条件的结果.");
	}
	// 双击添加到收件人列表
	//　必须每次执行完搜索后都要执行，否则相应的处理动作，没有注册
	$(".sequence_ip").dblclick(function(){
		$("#nocid").val($(this).attr("nocid"));
		$("#nocabbr").val($(this).attr("nocabbr"));
		$("#nocname").val($(this).attr("nocname"));
		$("#elementid").val($(this).attr("elementid"));
		$("#sequence").val($(this).attr("sequence"));
		$("#equipmentmodel").val($(this).attr("equipmentmodel"));
		$("#manage_ip").val($(this).attr("manage_ip"));
		$("#in_ip").val($(this).attr("in_ip"));
		$("#cpu").val($(this).attr("cpu"));
		$("#memory").val($(this).attr("memory"));
		$("#disk").val($(this).attr("disk"));
		close_resourcepool_div();
	});
}


function close_resourcepool_div(){
	$("#resourcepool_keyword").val("");
	$("#resourcepool_result_div").html("");
	$('#resourcepool_div').modal('hide');
}


function resourcepool_error(){
	$("#resourcepool_result_div").html("搜索失败或搜索超时.");	
}


function resourcepool_search(){
	var url = "/resourcepool/getServerByCMDB";
	resourcepool_keyword = $("#resourcepool_keyword").val();
	resourcepool_keyword = resourcepool_keyword.trim();
	var data = {'resourcepool_keyword':resourcepool_keyword}
        jQuery.ajax({
		type: 'GET',
		url: url,
		data: data,
		success: resourcepool_success,
		error:resourcepool_error,
		dataType: 'json',
		async:false
	});
}





