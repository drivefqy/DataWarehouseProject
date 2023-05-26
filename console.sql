-- 构建ODS层
create table ods_clientlog_full(
    ts bigint,
    mid int,
    cmd string,
    detail string,
    unknow string,
    unknow1 string
) partitioned by (dt string)
row format delimited fields terminated by "&"
location "/music/ods_clientlog_full"
tblproperties("compress"="gzip");
describe formatted ods_clientlog_full;
load data inpath "/datas/currentday_clientlog.tar.gz"
    overwrite into table  ods_clientlog_full partition (dt = "20200101" );
-- 创建song表
drop table ods_song_inc;
CREATE TABLE ods_song_inc (
  `source_id` string NOT NULL COMMENT '主键ID',
  `name` string  COMMENT '歌曲名字',
  `other_name` string  COMMENT '其他名字',
  `source` bigint  COMMENT '来源',
  `album` map<string,string> COMMENT '所属专辑',
  `product` string  COMMENT '发行公司',
  `language` string  COMMENT '歌曲语言',
  `video_format` string  COMMENT '视频风格',
  `duration` bigint  COMMENT '时长',
  `singer_info` array<string>  COMMENT '歌手信息',
  `post_time` string  COMMENT '发行时间',
  `pinyin_first` string  COMMENT '歌曲首字母',
  `pinyin` string  COMMENT '歌曲全拼',
  `singing_type` bigint  COMMENT '演唱类型',
  `original_singer` array<string>  COMMENT '原唱歌手',
  `lyricist` array<string>  COMMENT '填词',
  `composer` array<string>  COMMENT '作曲',
  `bpm` bigint  COMMENT 'BPM值',
  `star_level` bigint  COMMENT '星级',
  `video_quality` bigint  COMMENT '视频画质',
  `video_make` bigint  COMMENT '视频制作方式',
  `video_feature` bigint  COMMENT '视频画面特征',
  `lyric_feature` bigint  COMMENT '歌词字母特点',
  `Image_quality` bigint  COMMENT '画质评价',
  `subtitles_type` bigint  COMMENT '字幕类型',
  `audio_format` bigint  COMMENT '音频格式',
  `original_sound_quality` bigint  COMMENT '原唱音质',
  `original_track` bigint  COMMENT '音轨',
  `original_track_vol` bigint  COMMENT '原唱音量',
  `accompany_version` bigint  COMMENT '伴唱版本',
  `accompany_quality` bigint  COMMENT '伴唱音质',
  `acc_track_vol` bigint  COMMENT '伴唱音量',
  `accompany_track` bigint  COMMENT '伴唱音轨',
  `width` bigint  COMMENT '视频分辨率W',
  `height` bigint  COMMENT '视频分辨率H',
  `video_resolution` bigint  COMMENT '视频分辨率',
  `song_version` bigint  COMMENT '编曲版本',
  `authorized_company` string  COMMENT '授权公司',
  `status` bigint  COMMENT '状态',
  `publish_to` array<double>  COMMENT '产品类型'
) partitioned by (dt string)
row format delimited fields terminated by "|"
collection items terminated by ","
    map keys terminated by ":"
location "/music/ods_song_inc"
tblproperties("compress"="gzip");
load data inpath "/datas/data1" into table  ods_song_inc partition (dt = "20200101" );
select * from ods_song_inc;
CREATE TABLE ods_area_full (
  `cityID` int COMMENT '城市ID',
  `areaID`  int  COMMENT '区域ID',
  `area` string  COMMENT '区域'
) partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_area_full"
tblproperties("compress"="gzip");
load data inpath "/datas/datas/area_info.csv" into table  ods_area_full partition (dt = "20200101" );

drop table ods_city_full;
CREATE TABLE ods_city_full
(
    `provinceID` int COMMENT '省份ID',
    `cityID`     int COMMENT '城市ID',
    `city`       string COMMENT '城市'
)
partitioned by (dt string)
    row format delimited fields terminated by ","
location "/music/ods_city_full"
tblproperties("compress"="gzip");
load data inpath "/datas/datas/city_info.csv" into table  ods_city_full partition (dt = "20200101" );

CREATE TABLE ods_province_full
(
    `provinceID` int comment '自增唯一ID 省份ID',
    `province`   string COMMENT '省份'
)
partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_province_full"
tblproperties("compress"="gzip");
load data inpath "/datas/datas/province_info.csv" into table  ods_province_full partition (dt = "20200101" );

CREATE TABLE ods_machine_baseinfo_full (
  `mid` int COMMENT '机器ID',
  `serialnum` string  COMMENT '序列号',
  `hard_id` string  COMMENT 'Hard_ID',
  `song_warehouse_version` string  COMMENT '歌库版本号',
  `exec_version` string  COMMENT '系统版本号',
  `ui_version` string  COMMENT '歌库UI版本号',
  `online` string  COMMENT '是否在线',
  `status` int  COMMENT '状态',
  `current_login_time` string  COMMENT '最近登录时间',
  `pay_switch` string  COMMENT '支付开关是否打开',
  `language_type` int  COMMENT '语言类型',
  `songware_type` int  COMMENT '歌库类型',
  `screen_type` int  COMMENT '屏幕类型'
)
partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_machine_baseinfo_full"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/machine_baseinfo.csv"
overwrite into table  ods_machine_baseinfo_full partition (dt = "20200101" );

CREATE TABLE ods_machine_consume_detail_inc (
  `id` int  COMMENT '自增唯一ID',
  `mid` int  COMMENT '机器ID',
  `p_type` int  COMMENT '产品类型',
  `m_type` int  COMMENT '支付类型',
  `pkg_id` int  COMMENT '套餐ID',
  `pkg_name` string  COMMENT '套餐名称',
  `amount` int  COMMENT '总金额',
  `consum_id` string  COMMENT '消费ID',
  `order_id` string  COMMENT '订单ID',
  `trade_no` string  COMMENT '第三方交易编号',
  `action_time` string  COMMENT '消费时间',
  `uid` int  COMMENT '用户ID',
  `nickname` string  COMMENT '用户名称',
  `activity_id` int  COMMENT '优惠活动ID',
  `activity_name` string  COMMENT '优惠活动名称',
  `coupon_type` int  COMMENT '优惠券类型',
  `coupon_type_name` string  COMMENT '优惠券类型名称',
  `pkg_price` int  COMMENT '套餐原价',
  `pkg_discount` int  COMMENT '优惠金额',
  `order_type` int  COMMENT '订单类型',
  `bill_date` string  COMMENT '账单日期'
)
partitioned by (dt string)
    row format delimited fields terminated by ","
location "/music/ods_machine_consume_detail_inc"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/machine_consume_detail.csv"
overwrite into table  ods_machine_consume_detail_inc partition (dt = "20200101" );

CREATE TABLE ods_machine_local_info_full (
  `mid` int COMMENT '机器ID',
  `province_id` int COMMENT '省份ID',
  `city_id` int COMMENT '城市ID',
  `province` string COMMENT '省份',
  `city` string COMMENT '城市',
  `map_class` string COMMENT '地图返回标签',
  `mglng` string COMMENT '经度',
  `mglat` string COMMENT '纬度',
  `address` string COMMENT 'GPS地址',
  `real_address` string COMMENT '统一格式地址',
  `revenue_time` string COMMENT '运营时间',
  `sale_time` string COMMENT '销售时间'
)
partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_machine_local_info_full"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/machine_local_info.csv"
overwrite into table  ods_machine_local_info_full partition (dt = "20200101" );

-- 可能会用到的表
CREATE TABLE `ods_machine_store_info_full` (
  `id` int COMMENT '自增唯一ID',
  `store_name` int COMMENT '门店名称',
  `tag_code` int COMMENT '标签代码',
  `tag_name` int COMMENT '标签名称',
  `sub_tag_code` string COMMENT '子标签代码',
  `sub_tag_name` string COMMENT '子标签名称',
  `store_province_code` string COMMENT '门店省分代码',
  `store_city_code` string COMMENT '门店城市代码',
  `store_area_code` string COMMENT '门店区代码',
  `store_address` string COMMENT '门店地址',
  `ground_name` string COMMENT '场地名称',
  `store_opening_time` string COMMENT '门店开始营业时间',
  `store_closing_time` string COMMENT '门店结束营业时间',
  `sub_scence_cate_code` string COMMENT '子场景分类代码',
  `sub_scence_cate_name` string COMMENT '子场景分类名称',
  `sub_scence_code` string COMMENT '子场景代码',
  `sub_scence_name` string COMMENT '子场景名称',
  `brand_code` string COMMENT '品牌代码',
  `brand_name` string COMMENT '品牌名称',
  `sub_brand_code` string COMMENT '子品牌代码',
  `sub_brand_name` string COMMENT '子品牌名称'
)
partitioned by (dt string)
    row format delimited fields terminated by ","
location "/music/ods_machine_store_info_full"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/machine_store_info.csv"
overwrite into table  ods_machine_store_info_full partition (dt = "20200101" );

CREATE TABLE `ods_user_alipay_baseinfo_full` (
  `uid` int COMMENT '用户ID',
  `reg_mid` int COMMENT '注册机器ID',
  `sex` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `msisdn` string COMMENT '手机号码',
  `locationid` int COMMENT '地区ID',
  `mode_type` int COMMENT '注册登录模式',
  `regist_time` string COMMENT '注册时间',
  `user_exp` string COMMENT '用户当前经验值',
  `score` int COMMENT '累计积分',
  `user_level` int COMMENT '用户等级',
  `user_type` string COMMENT '用户类型',
  `is_certified` string COMMENT '实名认证',
  `is_student_certified` string COMMENT '是否学生',
  `openid` string COMMENT '支付宝ID'
  )
  partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_user_alipay_baseinfo_full"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/user_alipay_baseinfo.csv"
overwrite into table  ods_user_alipay_baseinfo_full partition (dt = "20200101" );

CREATE TABLE `ods_user_app_baseinfo_full` (
  `uid` int COMMENT '用户ID',
  `reg_mid` int COMMENT '注册机器ID',
  `sex` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `phone_number` string COMMENT '手机号码',
  `locationid` int COMMENT '地区ID',
  `regist_time` string COMMENT '注册时间',
  `user_exp` string COMMENT '用户当前经验值',
  `user_level` int COMMENT '用户等级',
  `app_uid` string COMMENT 'APPID'
)
partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_user_app_baseinfo_full"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/user_app_baseinfo.csv"
overwrite into table  ods_user_app_baseinfo_full partition (dt = "20200101" );

CREATE TABLE `ods_user_location_full` (
  `id` int  COMMENT 'ID',
  `uid` int COMMENT '用户ID',
  `x` string COMMENT '纬度',
  `y` string COMMENT '经度',
  `datetime` string COMMENT '时间',
  `mid` int COMMENT '机器ID'
  )
  partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_user_location_full"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/user_location.csv"
overwrite into table  ods_user_location_full partition (dt = "20200101" );

CREATE TABLE `ods_user_login_info_inc` (
  `id` int COMMENT '自增唯一主键',
  `uid` int COMMENT '用户ID',
  `mid` int COMMENT '机器ID',
  `logintime` string COMMENT '登录时间',
  `logouttime` string COMMENT '登出时间',
  `mode_type` int COMMENT '登录模式'
  )
  partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_user_login_info_inc"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/user_login_info.csv"
overwrite into table  ods_user_login_info_inc partition (dt = "20200101" );

CREATE TABLE `ods_user_qq_baseinfo_full` (
  `uid` int  COMMENT '用户ID',
  `reg_mid` int COMMENT '注册机器ID',
  `sex` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `msisdn` string COMMENT '手机号码',
  `locationid` int COMMENT '地区ID',
  `mode_type` int COMMENT '注册登录模式',
  `regist_time` string COMMENT '注册时间',
  `user_exp` string COMMENT '用户当前经验值',
  `score` int COMMENT '累计积分',
  `user_level` int COMMENT '用户等级',
  `openid` string COMMENT 'QQID'
  )
  partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_user_qq_baseinfo_full"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/user_qq_baseinfo.csv"
overwrite into table  ods_user_qq_baseinfo_full partition (dt = "20200101" );

CREATE TABLE `ods_user_wechat_baseinfo_full` (
  `uid` int COMMENT '用户ID',
  `reg_mid` int COMMENT '注册机器ID',
  `sex` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `msisdn` string COMMENT '手机号码',
  `locationid` int COMMENT '地区ID',
  `mode_type` int COMMENT '注册登录模式',
  `regist_time` string COMMENT '注册时间',
  `user_exp` string COMMENT '用户当前经验值',
  `score` int COMMENT '累计积分',
  `user_level` int COMMENT '用户等级',
  `wxid` string COMMENT '微信ID'
)
partitioned by (dt string)
row format delimited fields terminated by ","
location "/music/ods_user_wechat_baseinfo_full"
tblproperties("compress"="gzip");
load data  inpath "/datas/datas/user_wechat_baseinfo.csv"
overwrite into table  ods_user_wechat_baseinfo_full partition (dt = "20200101" );

-- 构建DIM层
-- 2020 全年日期
CREATE EXTERNAL TABLE dim_date
(
    `date_id`    STRING COMMENT '日期ID',
    `week_id`    STRING COMMENT '周ID,一年中的第几周',
    `week_day`   STRING COMMENT '周几',
    `day`        STRING COMMENT '每月的第几天',
    `month`      STRING COMMENT '一年中的第几月',
    `quarter`    STRING COMMENT '一年中的第几季度',
    `year`       STRING COMMENT '年份',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
)
row format delimited fields terminated by "\t";
load data inpath "/datas/date_info.txt" overwrite into table tmp;
insert overwrite table dim_date select * from tmp;
select * from dim_date;
drop table dim_address_full;
create table if not exists dim_address_full(
    provinceID int,
    province string,
    cityID int,
    city string,
    areaID int,
    area string
)
partitioned by (dt string)
stored as orc
location "/music/dim_address_full"
tblproperties("compress"="snappy");
set hive.exec.dynamic.partition.mode = nonstrict;
truncate table dim_address_full;
insert overwrite table  dim_address_full
select
     p.`provinceID`,
     p.province,
     nvl(c.cityID,0),
     nvl(c.city,"不存在"),
     nvl(a.areaID,0),
     nvl(a.area,"不存在"),
     "20200101"
from ods_province_full p
 left join ods_city_full c
on p.`provinceID` = c.`provinceID`
 left join ods_area_full a
on c.`cityID` = a.`cityID`;
select * from dim_address_full;
-- 创建歌曲表
drop table dim_song_full;
create table dim_song_full(
   NBR	string   comment "ID"
  ,NAME	string   comment "歌曲名字  "
  ,SOURCE	int   comment "来源      "
  ,ALBUM	string   comment "所属专辑  "
  ,PRDCT	string   comment "发行公司          "
  ,LANG	string   comment "歌曲语言          "
  ,VIDEO_FORMAT	string   comment "视频风格          "
  ,DUR	int   comment "时长                "
  ,SINGER1	string   comment "歌手1姓名        "
  ,SINGER2	string   comment "歌手2姓名        "
  ,SINGER1ID	string   comment "歌手1ID          "
  ,SINGER2ID	string   comment "歌手ID            "
  ,MAC_TIME	string   comment "加入机器时间   "
  ,POST_TIME	string   comment "发行时间          "
  ,PINYIN_FST	string   comment "歌曲首字母      "
  ,PINYIN	string   comment "歌曲全拼          "
  ,SING_TYPE	int   comment "演唱类型          "
  ,ORI_SINGER	string   comment "原唱歌手          "
  ,LYRICIST	string   comment "填词者             "
  ,COMPOSER	string   comment "作曲者             "
  ,BPM_VAL	int   comment "BPM值            "
  ,STAR_LEVEL	int   comment "星级                "
  ,VIDEO_QLTY	int   comment "视频画质          "
  ,VIDEO_MK	int   comment "视频制作方式   "
  ,VIDEO_FTUR	int   comment "视频画面特征   "
  ,LYRIC_FTUR	int   comment "歌词字母特点   "
  ,IMG_QLTY	int   comment "画质评价          "
  ,SUBTITLES_TYPE	int   comment "字幕类型          "
  ,AUDIO_FMT	int   comment "音频格式          "
  ,ORI_SOUND_QLTY	int   comment "原唱音质          "
  ,ORI_TRK	int   comment "音轨       "
  ,ORI_TRK_VOL	int   comment "原唱音量    "
  ,ACC_VER	int   comment "伴唱版本     "
  ,ACC_QLTY	int   comment "伴唱音质    "
  ,ACC_TRK_VOL	int   comment "伴唱音量     "
  ,ACC_TRK	int   comment "伴唱音轨    "
  ,WIDTH	int   comment "视频分辨率W   "
  ,HEIGHT	int   comment "视频分辨率H    "
  ,VIDEO_RSVL	int   comment "视频分辨率   "
  ,SONG_VER	int   comment "编曲版本     "
  ,AUTH_CO	int   comment "授权公司   "
  ,STATE	int   comment "状态  "
  ,PRDCT_TYPE	array<double>        comment "产品类型 "
)partitioned by (dt string)
stored as orc
location "/music/dim_song_full"
tblproperties("compress"="snappy");
insert overwrite table dim_song_full
select
source_id
,name
,source
,case when album["name"] = null then concat_ws("",map_keys(album))
    else album["name"] end
,nvl(product,"未知")
,language
,video_format
,duration
,split(singer_info[0],":")[1],
       split(singer_info[2],":")[1],
   split(singer_info[1],":")[1],
       split(singer_info[3],":")[1],
       "加入机器时间"
,post_time
,pinyin_first
,pinyin
,singing_type
,trim(regexp_replace(concat_ws(" ",original_singer),"id:\\d+|name:",""))
,trim(regexp_replace(concat_ws(" ",lyricist),"id:\\d+|name:",""))
,trim(regexp_replace(concat_ws(" ",composer),"id:\\d+|name:",""))
,bpm
,star_level
,video_quality
,video_make
,video_feature
,lyric_feature
,Image_quality
,subtitles_type
,audio_format
,original_sound_quality
,original_track
,original_track_vol
,accompany_version
,accompany_quality
,acc_track_vol
,accompany_track
,width
,height
,video_resolution
,song_version
,authorized_company
,status
,publish_to
,dt
from ods_song_inc;
set hive.vectorized.execution.enabled = false;
select * from dim_song_full;
show tables;
drop table dim_user_alipay_baseinfo_zip;
set hive.exec.dynamic.partition.mode = nonstrict;
CREATE TABLE `dim_user_alipay_baseinfo_zip` (
  `uid` int COMMENT '用户ID',
  `reg_mid` int COMMENT '注册机器ID',
  `sex` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `msisdn` string COMMENT '手机号码',
  `locationid` int COMMENT '地区ID',
  `mode_type` int COMMENT '注册登录模式',
  `regist_time` string COMMENT '注册时间',
  `user_exp` string COMMENT '用户当前经验值',
  `score` int COMMENT '累计积分',
  `user_level` int COMMENT '用户等级',
  `user_type` string COMMENT '用户类型',
  `is_certified` string COMMENT '实名认证',
  `is_student_certified` string COMMENT '是否学生',
  `openid` string COMMENT '支付宝ID',
  start_date string,
  end_date string
  )
  partitioned by (dt string)
stored as orc
location "/music/dim_user_alipay_baseinfo_zip"
tblproperties("compress"="snappy");
insert overwrite table  dim_user_alipay_baseinfo_zip
select
     *,
     "9999-12-31",
     "9999-12-31"
from ods_user_alipay_baseinfo_full;

CREATE TABLE `dim_user_app_baseinfo_zip` (
  `uid` int COMMENT '用户ID',
  `reg_mid` int COMMENT '注册机器ID',
  `sex` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `phone_number` string COMMENT '手机号码',
  `locationid` int COMMENT '地区ID',
  `regist_time` string COMMENT '注册时间',
  `user_exp` string COMMENT '用户当前经验值',
  `user_level` int COMMENT '用户等级',
  `app_uid` string COMMENT 'APPID',
  start_date string,
  end_date string
)
partitioned by (dt string)
stored as orc
location "/music/dim_user_app_baseinfo_zip"
tblproperties("compress"="snappy");
insert overwrite  table dim_user_app_baseinfo_zip
select
     *,
    "9999-12-31",
    "9999-12-31"
from ods_user_app_baseinfo_full;

CREATE TABLE `dim_user_location_zip` (
  `id` int  COMMENT 'ID',
  `uid` int COMMENT '用户ID',
  `x` string COMMENT '纬度',
  `y` string COMMENT '经度',
  `datetime` string COMMENT '时间',
  `mid` int COMMENT '机器ID',
  start_date string,
  end_date string
  )
  partitioned by (dt string)
stored as orc
location "/music/dim_user_location_zip"
tblproperties("compress"="snappy");
insert overwrite  table dim_user_location_zip
select
     *,
    "9999-12-31",
    "9999-12-31"
from ods_user_location_full;

CREATE TABLE `dim_user_qq_baseinfo_zip` (
  `uid` int  COMMENT '用户ID',
  `reg_mid` int COMMENT '注册机器ID',
  `sex` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `msisdn` string COMMENT '手机号码',
  `locationid` int COMMENT '地区ID',
  `mode_type` int COMMENT '注册登录模式',
  `regist_time` string COMMENT '注册时间',
  `user_exp` string COMMENT '用户当前经验值',
  `score` int COMMENT '累计积分',
  `user_level` int COMMENT '用户等级',
  `openid` string COMMENT 'QQID'
 ,start_date string,
  end_date string
  )
  partitioned by (dt string)
stored as orc
location "/music/dim_user_qq_baseinfo_zip"
tblproperties("compress"="snappy");
insert overwrite  table dim_user_qq_baseinfo_zip
select
     *,
    "9999-12-31",
    "9999-12-31"
from ods_user_qq_baseinfo_full;

CREATE TABLE `dim_user_wechat_baseinfo_zip` (
  `uid` int COMMENT '用户ID',
  `reg_mid` int COMMENT '注册机器ID',
  `sex` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `msisdn` string COMMENT '手机号码',
  `locationid` int COMMENT '地区ID',
  `mode_type` int COMMENT '注册登录模式',
  `regist_time` string COMMENT '注册时间',
  `user_exp` string COMMENT '用户当前经验值',
  `score` int COMMENT '累计积分',
  `user_level` int COMMENT '用户等级',
  `wxid` string COMMENT '微信ID'
,start_date string,
  end_date string
  )
  partitioned by (dt string)
stored as orc
location "/music/dim_user_wechat_baseinfo_zip"
tblproperties("compress"="snappy");
insert overwrite  table dim_user_wechat_baseinfo_zip
select
     *,
    "9999-12-31",
    "9999-12-31"
from ods_user_wechat_baseinfo_full;

drop table dim_user_all_zip;
create table dim_user_all_zip(
    uid int,
    sex string,
    birthday string,
    msisdn string,
    login_type string,
    regist_time string,
    score int,
    user_level int,
    account_id string,
    locationid string,
    x double,
    y double,
    location_dt string,
    start_dt string,
    end_dt string
) partitioned by (dt string)
stored as orc
location "/music/dim_user_all_zip"
tblproperties("compress"="snappy");
insert overwrite table dim_user_all_zip
select
 a.uid,
 case when sex =1 then "女"
     when sex =2 then "男"
         else "未知" end,
       birthday,msisdn,type,regist_time,score,user_level,
       openid,locationid,l.x,l.y,l.datetime,"2020-01-01","9999-12-31","9999-12-31"
from ods_user_location_full l
right join (
select  uid,sex,birthday,msisdn,"QQ" as type,regist_time,score,user_level,openid,locationid
from dim_user_qq_baseinfo_zip
union
select uid,sex,birthday,msisdn,"微信" as type,regist_time,score,user_level,wxid,locationid
from ods_user_wechat_baseinfo_full
union
select  uid,sex,birthday,phone_number,"APP" as type,regist_time,null,user_level,app_uid,locationid
from ods_user_app_baseinfo_full
union
select uid,sex,birthday,msisdn,"支付宝" as type,regist_time,score,user_level,openid,locationid
from ods_user_alipay_baseinfo_full
    ) a on l.uid = a.uid;
select * from dim_user_all_zip;
-- 机器基本信息维度表
drop table  dim_machine_baseinfo_full
CREATE TABLE dim_machine_baseinfo_full (
  `mid` int COMMENT '机器ID',
  `serialnum` string  COMMENT '序列号',
  `hard_id` string  COMMENT 'Hard_ID',
  `song_warehouse_version` string  COMMENT '歌库版本号',
  `exec_version` string  COMMENT '系统版本号',
  `ui_version` string  COMMENT '歌库UI版本号',
  `online` string  COMMENT '是否在线',
  `status` int  COMMENT '状态',
  `current_login_time` string  COMMENT '最近登录时间',
  `pay_switch` string  COMMENT '支付开关是否打开',
  `language_type` int  COMMENT '语言类型',
  `songware_type` int  COMMENT '歌库类型',
  `screen_type` int  COMMENT '屏幕类型'
)partitioned by (dt string)
stored as orc
location "/music/dim_machine_baseinfo_full"
tblproperties("compress"="snappy");
insert overwrite table dim_machine_baseinfo_full
select * from ods_machine_baseinfo_full;

-- 机器位置表可以不要
drop table  dim_machine_local_info_full
CREATE TABLE dim_machine_local_info_full (
  `mid` int COMMENT '机器ID',
  `province_id` int COMMENT '省份ID',
  `city_id` int COMMENT '城市ID',
  `province` string COMMENT '省份',
  `city` string COMMENT '城市',
  `map_class` string COMMENT '地图返回标签',
  `mglng` string COMMENT '经度',
  `mglat` string COMMENT '纬度',
  `address` string COMMENT 'GPS地址',
  `real_address` string COMMENT '统一格式地址',
  `revenue_time` string COMMENT '运营时间',
  `sale_time` string COMMENT '销售时间'
)partitioned by (dt string)
stored as orc
location "/music/dim_machine_local_info_full"
tblproperties("compress"="snappy");
insert overwrite table dim_machine_local_info_full
select * from ods_machine_local_info_full;

set hive.exec.dynamic.partition.mode = nonstrict;  -- 动态分区
-- 构建DWD层
drop table dwd_song_play_inc;
create table dwd_song_play_inc(
     songid string,
     mid string comment "机器id",
     optrate_type int,
     uid string comment "用户"
                ,consume_type string
                 ,play_time  int comment "播放时间"
    ,dur_time int comment "时长"
   ,session_id string comment "局数ID",
   songname string
    ,pkg_id string
    ,order_id string,
    ts bigint
)partitioned by (dt string)
    ROW FORMAT delimited fields terminated by ","
location "/music/dwd_song_play_inc";
insert overwrite table dwd_song_play_inc
select
        t.songid,
        t.mid,
        t.optrate_type,
        t.uid,
        t.consume_type,
        t.play_time,
        t.dur_time,
        t.session_id,
        t.songname,
        t.pkg_id,
        t.order_id,
              ts *1000,
       "2020-01-01"
from ods_clientlog_full
lateral view json_tuple(detail,"songid",
 "mid",
"optrate_type",
"uid",
"consume_type",
"play_time",
"dur_time",
"session_id",
"songname",
"pkg_id",
"order_id") t as
        songid,
         mid,
        optrate_type,
        uid,
        consume_type,
        play_time,
        dur_time,
        session_id,
        songname,
        pkg_id,
        order_id
where cmd like "MINIK_CLIENT_SONG_PLAY%";

create external table dwd_user_login_inc(
        id int
        ,uid int
        ,mid int
        ,logintime string
        ,logouttime string
        ,mode_type int
) partitioned by (dt string)
stored as orc
location "/music/dwd_user_login_inc"
tblproperties("compress"="snappy");
insert overwrite table dwd_user_login_inc
select
    id,
    uid,
    mid,
    concat(
       concat_ws(
            "-",
            substring(logintime,1,4),
            substring(logintime,5,2),
            substring(logintime,7,2)),
            " ",
       concat_ws(
                ":",
                substring(logintime,9,2),
                substring(logintime,11,2),
                substring(logintime,13,2)
           )) as a,
      concat(
       concat_ws(
            "-",
            substring(logouttime,1,4),
            substring(logouttime,5,2),
            substring(logouttime,7,2)),
            " ",
       concat_ws(
                ":",
                substring(logouttime,9,2),
                substring(logouttime,11,2),
                substring(logouttime,13,2)
           ) )as b,
          mode_type,
       "2020-01-01"
from ods_user_login_info_inc;
select * from dwd_user_login_inc;
-- 创建机器位置表
drop table dwd_machine_local_full;
create external table dwd_machine_local_full(
        mid int
        ,province_id int
        ,city_id int
        ,province string
        ,city string
        ,map_class string
        ,address string
        ,real_address string
        ,revenue_time string
        ,sale_time string
) partitioned by (dt string)
stored as orc
location "/music/dwd_machine_local_full"
tblproperties("compress"="snappy");
insert overwrite table dwd_machine_local_full
select
         mid
        ,province_id
        ,city_id
        ,province
        ,city
        ,map_class
        ,address
        ,real_address
        ,
       concat_ws(
            "-",
            substring(revenue_time ,1,4),
            substring(revenue_time ,5,2),
            substring(revenue_time ,7,2)) as a,
       concat_ws(
            "-",
            substring(sale_time ,1,4),
            substring(sale_time ,5,2),
            substring(sale_time ,7,2))as b,
       "2020-01-01"
from ods_machine_local_info_full;
select * from dwd_machine_local_full;
-- 机器消费表
drop table dwd_machine_consume_inc;
create external table dwd_machine_consume_inc (
    mid	int
    ,p_type	int
    ,m_type	int
    ,pkg_id	int
    ,pkg_name	string
    ,amount	int
    ,action_time string
    ,uid	int
    ,nickname	string
    ,activity_id	int
    ,activity_name	string
    ,coupon_type	int
    ,coupon_type_name	string
    ,pkg_price	int,
    pkg_discount int,
    bill_date string
)
partitioned by (dt string)
stored as orc
location "/music/dwd_machine_consume_full"
tblproperties("compress"="snappy");
insert overwrite table dwd_machine_consume_inc
select
     mid
    ,p_type
    ,m_type
    ,pkg_id
    ,pkg_name
    ,amount
    ,action_time
    ,uid
    ,nickname
    ,activity_id
    ,activity_name
    ,coupon_type
    ,coupon_type_name
    ,pkg_price,
    pkg_discount,
    bill_date,
       "2020-01-01"
from ods_machine_consume_detail_inc;

show tables like "ods*" ;

-- 构建DWS层 轻量级聚合
-- 时间戳 时区转换函数
select date_format(
     from_utc_timestamp(unix_timestamp() * 1000,"GMT+8"),  ---默认是零时区，中国东八区
    "yyyy-MM-dd HH:mm:ss");
-- 数据最新为2019-12-03,数仓分区为2020-01-01

-- 最近一天歌曲热度：
drop table dws_last_day_song_singer;
create table dws_last_day_song_singer(
    songname
    string ,songid
    string ,dur
    string ,optrate_type
    string ,album
    string ,SINGER1
    string ,SINGER1ID
    string ,SINGER2
    string ,SINGER2ID
    string ,composer
    string,lyricist
    string,uid string,
    order_id string,
    cnt int
)partitioned by (dt string)
stored as orc
location "/music/dws_last_day_song_singer"
tblproperties("compress"="snappy");
insert overwrite table dws_last_day_song_singer
select
    songname,
    songid,
    dur,
    optrate_type,
    album,
    SINGER1,
    SINGER1ID,
    SINGER2,
    SINGER2ID,
    composer,
    lyricist,
    uid,
    order_id,
    count(1) as cnt,
       "2020-01-01"
from (
select
    dwd.songname,
    dwd.songid,
    dim.dur,
    dwd.optrate_type,
    dim.album,
    dim.SINGER1,
    dim.SINGER1ID,
    dim.SINGER2,
    dim.SINGER2ID,
    dim.composer,
    dim.lyricist,
    dwd.uid,
    dwd.order_id,
    date_format(
        from_utc_timestamp( ts,"GMT+8"),  ---默认是零时区，中国东八区
        "yyyy-MM-dd") as dt
from dim_song_full dim
join  dwd_song_play_inc dwd
on dim.NBR = dwd.songid
) result where dt between date_sub("2019-12-03",30) and "2019-12-03"
group by
songname,
songid,
dur,
optrate_type,
album,
SINGER1,
SINGER1ID,
SINGER2,
SINGER2ID,
composer,
lyricist,
uid,
order_id;
select * from dws_last_day_song_singer;

-- 最近七天歌曲热度：
drop table dws_last_week_song_singer;
create table dws_last_week_song_singer(
    songname
    string ,songid
    string ,dur
    string ,optrate_type
    string ,album
    string ,SINGER1
    string ,SINGER1ID
    string ,SINGER2
    string ,SINGER2ID
    string ,composer
    string,lyricist
    string,uid string,
    order_id string,
    cnt int
)partitioned by (dt string)
stored as orc
location "/music/dws_last_week_song_singer"
tblproperties("compress"="snappy");
insert overwrite table dws_last_week_song_singer
select
    songname,
    songid,
    dur,
    optrate_type,
    album,
    SINGER1,
    SINGER1ID,
    SINGER2,
    SINGER2ID,
    composer,
    lyricist,
    uid,
    order_id,
    count(1) as cnt,
       "2020-01-01"
from (
select
    dwd.songname,
    dwd.songid,
    dim.dur,
    dwd.optrate_type,
    dim.album,
    dim.SINGER1,
    dim.SINGER1ID,
    dim.SINGER2,
    dim.SINGER2ID,
    dim.composer,
    dim.lyricist,
    dwd.uid,
    dwd.order_id,
    date_format(
        from_utc_timestamp( ts,"GMT+8"),  ---默认是零时区，中国东八区
        "yyyy-MM-dd") as dt
from dim_song_full dim
join  dwd_song_play_inc dwd
on dim.NBR = dwd.songid
) result where dt between date_sub("2019-12-03",7) and "2019-12-03"
group by
songname,
songid,
dur,
optrate_type,
album,
SINGER1,
SINGER1ID,
SINGER2,
SINGER2ID,
composer,
lyricist,
uid,
order_id;
select * from dws_last_week_song_singer;

-- 最近三十天歌曲热度：
drop table dws_last_month_song_singer;
create table dws_last_month_song_singer(
    songname
    string ,songid
    string ,dur
    string ,optrate_type
    string ,album
    string ,SINGER1
    string ,SINGER1ID
    string ,SINGER2
    string ,SINGER2ID
    string ,composer
    string,lyricist
    string,uid string,
    order_id string,
    cnt int
)partitioned by (dt string)
stored as orc
location "/music/dws_last_month_song_singer"
tblproperties("compress"="snappy");
insert overwrite table dws_last_month_song_singer
select
    songname,
    songid,
    dur,
    optrate_type,
    album,
    SINGER1,
    SINGER1ID,
    SINGER2,
    SINGER2ID,
    composer,
    lyricist,
    uid,
    order_id,
    count(1) as cnt,
       "2020-01-01"
from (
select
    dwd.songname,
    dwd.songid,
    dim.dur,
    dwd.optrate_type,
    dim.album,
    dim.SINGER1,
    dim.SINGER1ID,
    dim.SINGER2,
    dim.SINGER2ID,
    dim.composer,
    dim.lyricist,
    dwd.uid,
    dwd.order_id,
    date_format(
        from_utc_timestamp( ts,"GMT+8"),  ---默认是零时区，中国东八区
        "yyyy-MM-dd") as dt
from dim_song_full dim
join  dwd_song_play_inc dwd
on dim.NBR = dwd.songid
) result where dt between date_sub("2019-12-03",30) and "2019-12-03"
group by
songname,
songid,
dur,
optrate_type,
album,
SINGER1,
SINGER1ID,
SINGER2,
SINGER2ID,
composer,
lyricist,
uid,
order_id;
select * from dws_last_month_song_singer;

-- 1.省份机器数量日统计
create table dws_machine__province_total(
    province string,
    city string,
    sale_time string,
    cnt int
)
partitioned by (dt string)
stored as orc
location "/music/dws_machine__province_total"
tblproperties("compress"="snappy");
insert overwrite table dws_machine__province_total
select
    province,
    city,
    sale_time,
    count(1) as cnt,
       "2020-01-01"
from dwd_machine_local_full dwd
group by
province,
city,
sale_time;

-- 2.机器详细信息日统计
drop table dws_machine_detail_total;
create table dws_machine_detail_total(
      mid
      string ,song_warehouse_version
      string ,ui_version
      string ,status
      string ,pay_switch
      string ,exec_version
      string ,online
      string ,screen_type
      string ,province
      string ,city
      string ,map_class
      string ,sale_time
      string,
      current_time string
)partitioned by (dt string)
stored as orc
location "/music/dws_machine_detail_total";
insert overwrite table dws_machine_detail_total
select
       dim.mid,
       dim.song_warehouse_version,
       dim.ui_version,
       dim.status,
       dim.pay_switch,
       dim.exec_version,
       dim.online,
       dim.screen_type,
       dwd.province,
       dwd.city,
       dwd.map_class,
       dwd.sale_time,
       concat(
       concat_ws(
            "-",
            substring(dim.current_login_time,1,4),
            substring(dim.current_login_time,5,2),
            substring(dim.current_login_time,7,2)),
            " ",
       concat_ws(
                ":",
                substring(dim.current_login_time,9,2),
                substring(dim.current_login_time,11,2),
                substring(dim.current_login_time,13,2)
           )) as a,
       "2020-01-01"
from dim_machine_baseinfo_full dim
join dwd_machine_local_full dwd
on dwd.mid = dim.mid;
select * from dws_machine_detail_total;
set hive.auto.convert.join=false;
set hive.vectorized.execution.enabled = false;
-- 每天用户登录的信息
create table dws_user_login(
    uid string,
    login_date string,
    logout_date string,
    cnt int,
    user_level int,
    birthday string,
    sex string ,
    msisdn  string ,
    login_type string
)partitioned by (dt string)
stored as orc
location "/music/dws_user_login"
tblproperties ("compress"="snappy");
insert overwrite table dws_user_login
select
     t.*,
     dim.user_level,
     dim.birthday,
     dim.sex,
     concat(substring(dim.msisdn,1,3),"****",substring(dim.msisdn,7,4)),
     dim.login_type,
     "2020-01-01"
from (
select
    uid,
    date_format(logintime,"yyyy-MM-dd"),
    date_format(logouttime,"yyyy-MM-dd"),
    count(1) as cnt
from dwd_user_login_inc
group by
date_format(logintime,"yyyy-MM-dd"),
date_format(logouttime,"yyyy-MM-dd"),
uid
) t
left join (
select * from dim_user_all_zip  where dt = "9999-12-31"
) dim
on t.uid = dim.uid;
select * from dws_user_login;
-- 机器消费表
create table dws_machine_consume_1d(
     mid string,
     uid string,
     pkg_name string,
     pkg_price int,
     bill_date int,
     amount int,
     address_type string,
     sale_time string,
     address string,
     login_type string,
     gender string,
     user_level int,
     birthday string
)
partitioned by (dt string)
stored as orc
location "/music/dws_machine_consume_1d"
tblproperties ("compress"="snappy");
insert overwrite table dws_machine_consume_1d
select
     t.*,
     split(dwd.map_class,",")[1],
     dwd.sale_time,
     concat_ws("-",dwd.province,dwd.city,dwd.real_address),
     dim.login_type,
     nvl(dim.sex,"未知"),
     nvl(dim.user_level,0),
     nvl(dim.birthday,"未知"),
       "2020-01-01"
from (
select
     mid,
     uid,
     pkg_name,
     pkg_price,
     bill_date,
     sum(amount)
from dwd_machine_consume_inc
group by
mid,
uid,
pkg_name,
amount,
pkg_price,
bill_date
) t left join dwd_machine_local_full dwd
on dwd.mid = t.mid
left join (
select * from dim_user_all_zip where  dt ="9999-12-31") dim
on dim.uid = t.uid;
select * from dws_machine_consume_1d ;

-- ADS层
-- 1日歌曲热度、歌手热度 取前30
create table ads_song_1d(
    songid string,
    songname string,
    cnt int,
    dt string
)location "/music/ads_song_1d";
insert overwrite table ads_song_1d
select dws.songid,dws.songname,count(*),"2020-01-01" as cnt
from dws_last_day_song_singer dws group by songname,dws.songid
order by cnt desc limit 20;
-- 30日歌曲热度、歌手热度 取前30
create table ads_song_1month(
    songid string,
    songname string,
    cnt int,
    months_number int
)location "/music/ads_song_1month";
insert into table ads_song_1month
select dws.songid,dws.songname,count(*) as cnt,1
from dws_last_month_song_singer dws group by songname,dws.songid
order by cnt desc limit 30;

-- 7日歌曲热度、歌手热度 取前30
create table ads_song_1week(
    songid string,
    songname string,
    cnt int,
    weeks_number int
)location "/music/ads_song_1week";
insert into table ads_song_1week
select dws.songid,dws.songname,count(*) as cnt,1
from dws_last_week_song_singer dws group by songname,dws.songid
order by cnt desc limit 30;

-- 省份机器数量日统计
create table ads_machine_province_1d(
    province string,
    cnt int
)location "/music/ads_machine_province_1d";
insert overwrite table ads_machine_province_1d
select province,sum(cnt) from dws_machine__province_total
group by province
-- 机器详细信息日统计
create table ads_machine_detail_1d
as
select * from dws_machine_detail_total;

-- 统计7日活跃用户信息
create table ads_user_login_1week
as
select
     uid
from (
select
     uid,
     login_date,
     lead(login_date,6,"9999-12-31") over(partition by uid order by login_date)  as nd
from dws_user_login
group by uid,login_date
) t where date_add(login_date,7) = nd
group by uid;

--统计每台机器的消费
create table ads_machine_income
as
select mid,sum(amount) from dws_machine_consume_1d
group by mid;












