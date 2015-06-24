//右键菜单
function open_context_menu(e,node){
	e.preventDefault();
	$("#mm").menu('show', {left: e.pageX,top: e.pageY  }); 
	$("#hide_id").val(node.id);
} 
// 右键菜单点击动作的处理函数
function context_menu_click(item){  
	var node_id = parseInt($("#hide_id").val());
	var node = $('#show_tree').tree('find', node_id);
	$("#hide_action").val(item.id);
	if(item.id=='add'){
		if(node.attributes.level>=3){
			alert('问题类目不能超过3级');
			return;
		}
		$('#node_div').modal({show:true,backdrop:'static'});

	}else if(item.id=='update'){
		//初始化，过去的设置内容
		init_node_div(node);
		$('#node_div').modal({show:true,backdrop:'static'});

	}else if(item.id=='remove'){
		//处理前端展示
		$("#show_tree").tree('remove',node.target);
		//处理后台数据
		node_remove(node);
	}
}
//清理增加、编辑节点面板
function clear_node_div(){
	$('#node_div').modal('hide');
	$("#hide_id").val("");
	//-----------节点配置面板------------------
	
	//节点名称
	$("#curr_node").val("");
}
//编辑节点----初始化node_div
function init_node_div(node){
	$("#curr_node").val(node.text);
}
//删除某个已选定的图表
function remove_item(obj){
	$(obj).parent().parent().remove();
}
//新增节点和编辑节点都使用此函数　编辑节点，同时提交节点和图表的对应关系
function node_confirm_click(){
	var node_id = parseInt($("#hide_id").val());
	var node = $('#show_tree').tree('find', node_id);
	var node_text = $("#curr_node").val();

	var action = $("#hide_action").val()
	if(action == 'add'){
		//处理前端展示
		$("#show_tree").tree('append', {
			parent: node.target,
			data: [{"id":"undefined","text": node_text,"attributes":{"level":node.attributes.level + 1}}]
		});
		//处理后台数据
		node_add(node, node_text);
	}else if(action == 'update'){
		//处理前端展示
		$("#show_tree").tree('update', {
			target: node.target,
			text: node_text
		});
		//处理后台数据
		node_update(node,node_text);
		
	}
	//清理node_div
	clear_node_div();
}

function node_add(parent_node, node_text){
    jQuery.ajax({
		type: 'POST',
		data:{'action':'append','parent_id':parent_node.id,'text':node_text},
		url: "/problem/manipulate_tree/",
		success: append_success,
		error:manipulate_error,
		dataType: 'json',
		async:false
	});	
}
function node_update(node,node_text){
    jQuery.ajax({
		type: 'POST',
		data:{'action':'update','node_id':node.id,'text':node_text},
		url: "/problem/manipulate_tree/",
		success: manipulate_success,
		error:manipulate_error,
		dataType: 'json',
		async:false
	});	
}
function node_remove(node){
    jQuery.ajax({
		type: 'POST',
		data:{'action':'delete','node_id':node.id},
		url: "/problem/manipulate_tree/",
		success: manipulate_success,
		error:manipulate_error,
		dataType: 'json',
		async:true
	});	
}
function manipulate_success(data){
	alert("操作成功!");
}
function append_success(data){
	node = $('#show_tree').tree('find', 'undefined');
	$("#show_tree").tree('update', {
		target: node.target,
		id: data.node_id
	});
	alert("操作成功!");
}
function manipulate_error(data){
	alert("操作失败!");
}
