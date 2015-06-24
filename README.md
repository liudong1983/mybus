# mybus
实现MySQL数据库到Redis,以及HBASE的全量，以及增量同步
1. 支持通过正则表达式指定需要导出的db以及表
2. bus程序无状态，每一行有自己的位置点，位置点信息存储在下游db中
3. 增量同步通过解析MySQL的行复制日志，完成增量数据的同步
4. 通过编写so，实现转换的业务逻辑
5. 上下游ip，端口，需要导出的schema信息都存储在configservice中，configservice是一个用redis作为存储的基于django的pythonweb程序

