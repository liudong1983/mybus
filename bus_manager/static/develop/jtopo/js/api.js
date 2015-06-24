
(function(){
	var ApiDoc = [
 		'核心对象', 
		{
			name:'jTopo.Stage', 
			text:'一个抽象的舞台对象,对应一个Canvas对象，所有图形展示的地方',
			functions:[
				{
					name: 'frames',
					text:'设置当前舞台播放的帧数/秒',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				}
			]
		},
		{
			name:'jTopo.Scene', 
			text:'场景对象',
			functions:[
				{
					name: '.add()',
					text: '添加对象到当前场景中来, 例如：scene.add(new JTopo.Node()); scene.add(new JTopo.Link(nodeA, nodeZ))',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: '.remove()',
					text: '删除场景中的某个元素, 例如：scene.remove(myNode);',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: '.background',
					text: '设置场景的背景图片, 例如：scene.background = "./img/bg.png"',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},	
				{
					name: '.addEventListener()',
					text: '监听场景事件，例如：scene.addEventListener("mousedown", function(event){})',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},				
				{
					name: 'visible',
					text:'得到、设置场景是否可见',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'background',
					text:'获取场景的背景',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				}
			]
		},
		{
			name:'jTopo.Node', 
			text:'节点对象',
			functions:[				
				{
					name: 'text',
					text: '设置节点的名字（显示文本）',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'setImage()',
					text: '设置节点图片',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'setSize()',
					text:'设置节点的宽和高',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'getSize()',
					text:'获取节点的宽和高',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'setBound()',
					text:'设置节点的坐标、宽、高',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'getBound()',
					text:'获取节点的坐标、宽、高',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'setLocation()',
					text:'设置节点在场景中的位置坐标(左上角）',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},				
				{
					name: 'setCenterLocation()',
					text:'设置节点在场景中的位置坐标(中心位置)',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'addEventListener()',
					text: '监听节点事件，如：node.addEventListener("mousedown", function(event){})',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'x',
					text: '节点x坐标值',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'y',
					text: '节点y坐标值',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'setSize()',
					text:'设置节点的宽和高',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'visible',
					text:'设置节点是否可见',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'dragable',
					text:'设置节点是否可以拖动',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'rotate',
					text:'设置节点旋转的角度（弧度）',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'alpha',
					text:'设置节点透明度',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'scalaX',
					text:'设置节点水平缩放',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'scalaY',
					text:'设置节点垂直缩放',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'fillColor',
					text:'设置节点的填充颜色',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				}
			]
		},		
		{
			name:'jTopo.Link', 
			text:'连线对象',
			functions:[				
				{
					name: 'name',
					text: '连线的名字（文本）',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'nodeA',
					text: '起始节点对象',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'nodeZ',
					text: '终止节点对象',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'alpha',
					text: '透明度',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'style.strokeStyle',
					text: '连线的颜色',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'style.lineWidth',
					text: '线条的宽度（像素）',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				}
			]
		},// end jTopo.Link
		{
			name:'jTopo.Container', 
			text:'容器对象',
			functions:[				
				{
					name: 'name',
					text: '名称（文本）, 不会显示',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'x',
					text: 'x坐标值',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'y',
					text: 'y坐标值',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'width',
					text: '容器宽度',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'height',
					text: '容器高度',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'alpha',
					text: '透明度',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'dragble',
					text:'是否可以拖动',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				}
			]
		}, // end jTopo.Container
		{
			name:'jTopo.Effect.Animate', 
			text:'动画效果',
			functions:[				
				{
					name: 'gravity()',
					text: '给指定元素增加重力效果',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				},
				{
					name: 'stepByStep()',
					text: '通用动画效果功能，可以把一个元素对象的某些属性在指定的时间内变化到指定值',
					support: 'IE, Firefox, Safari, Opera, Chrome'
				}
			]
		}
	];

	
	
	function drawMenus(menus){
		var ul = $('#menu').empty();
		var li = null;
		var children = null;
		$.each(ApiDoc, function(i, e){
			if(typeof e == 'string'){
				li = $('<li>').appendTo(ul).addClass('cat-item cat-item-1').appendTo(ul);
				$('<a>').html(e).appendTo(li);
				children = $('<ul>').addClass('children').appendTo(li);
			}else{
				var cli = $('<li>').addClass('cat-item cat-item-5').appendTo(children);
				$('<a>').attr('href', '#').html(e.name).appendTo(cli).click(function(){
					showFunctions(e);
				});
			}
		});
	}

	function showFunctions(obj){
		var div = $('#content').empty();
		$('<h1>').addClass('page-title').html('API: ' + obj.name).appendTo(div);
		$('<hr>').appendTo(div);
		$('<span>').html(obj.text).appendTo(div);
		
		for(var i=0; i<obj.functions.length; i++){
			var fn = obj.functions[i];
			createArtical(fn).appendTo(div);
		}
	}

	function createArtical(f){
		var article = $('<article>').addClass('hentry');
		var header = $('<header>').addClass('entry-header').appendTo(article);
		var div = $('<div>').addClass('entry-meta').appendTo(header);
		var span = $('<span>').addClass('category').appendTo(div);
		$('<a>').attr('href', '#').html('浏览器 &gt;').appendTo(span);
		$('<a>').attr('href', '#').html(f.support).appendTo(span);

		var h1 = $('<h1>').addClass('entry-title').appendTo(header);
		$('<a>').attr('href', '#').html(f.name).appendTo(h1);
		
		$('<div>').addClass('entry-summary').append($('<p>').html(f.text)).appendTo(article);
		return article;
	}

	$(document).ready(function(){
		drawMenus(ApiDoc);
		$('#menu a:nth(1)').click();
	});

})($ || jQuery);
