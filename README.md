# mybus
实现MySQL数据库到Redis,以及HBASE的全量，以及增量同步  
1. 支持通过正则表达式指定需要导出的db以及表  
2. bus程序无状态，每一行有自己的位置点，位置点信息存储在下游db中  
3. 增量同步通过解析MySQL的行复制日志，完成增量数据的同步  
4. 通过编写so，实现转换的业务逻辑   
5. 上下游ip，端口，需要导出的schema信息都存储在configservice中，configservice是一个用redis作为存储的基于django的pythonweb程序  

程序作为基础组件，在新浪以及微博的数据运维中，其可靠性得到了充分验证

使用方式如下：  
一. 编译程序   
1. 代码中包含了链接mysql以及hbase的库，目前支持centos5/centos6,centos7暂不支持   
2. 进入mybus目录，执行make命令，如果make失败，请将结果贴出来    

二. 启动configservice  
2. redis官网下载redis随便一个版本, 安装，并运行，得到redis_ip, redis_port，暂不支持rediscluster    
3. 进入bus_manager/business目录，修改views.py文件,填入具体的redis_ip, redis_port   
4. 然后返回bus_manger目录，执行nohup python manage.py runserver 0.0.0.0:8888 &   
    

三. 配置msg_test模拟业务   
1. 打开浏览器，执行http://host:8888, 打开configservice页面      
2. 添加业务msg_test  
3. 配置msg_test,配置较为复杂，后面会详细介绍，字符集转换慎用，最好下游和上游数据集一致   
4. 配置schema的时候，db和table的名字请用正则表达式，对于列，请采用c1#c2#c3的形式，统计开关，用来确定这个表的数据是否要统计    


四. 编译转换插件并copy到具体位置 
1. 进入myso/msg 目录下，执行make命令，生成libprocess.so       
2. 进入src目录，执行mkdir msg && cp -ap ../myso/msg/libprocess.so  msg   
3. 进入src目录，执行/usr/local/databus/bus -x msg_test -p 18889 -m 127.0.0.1:9999    
  

五. 建立上游数据库,我们选用的mysql版本为5.5     
1. 启动mysql数据库，建立要导出的heart_beat表     

六. 建立下游数据库,redis数据库版本没有具体要求   
1. 启动redis数据库   

七. 根据设定配置，执行命令    
1. 在configservice页面选择info source,检查下上下游配置是否正确   
2. 在configservice页面选择info matchschema,检查导出对应的schema信息是否正确    
2. 然后执行启动全量复制就开始全量复制，全量后要增量复制，修改选项，再执行启动增量复制就可以了   
3. 选择info, 检查是否有error信息    
   

这里说的这些对于真正要run起来还是不太够的，我们的思路其实很清晰   
1. 所有的配置都要存在configservice中，所以所有的配置都在页面上完成，配置有些复杂    
2. bus启动不需要配置文件，但是需要制定configservice的dns和port    
3. 我们的底层运维系统采用了salt, 我们的web服务器运行在master上，所以bus的启动，日志查看都在web页面上完成，
同时bus，支持采用redis协议对其监控，启动关闭传输，关闭程序等操作   

