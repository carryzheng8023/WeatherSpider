use csust;
create table if not exists dm_qx_weather_spider(
  qx_id varchar(20) primary key not null comment '主键',
  area varchar(10) comment '区域名称',
  wd double default 0.0 comment '温度',
  sd double default 0.0 comment '湿度',
  js double default 0.0 comment '降水',
  fx varchar(5) comment '风向',
  fl double default 0.0 comment '风力',
  fs double default 0.0 comment '风速',
  fzy double default 0.0 comment '风资源',
  kqzl double default 0.0 comment '空气质量',
  create_time bigint comment '采集时间',
  modify_time bigint comment '修改时间',
  qx_date bigint comment '气象日期',
  is_error int comment '是否为错误数据：0正常，1错误'
) comment '湖南天气数据';