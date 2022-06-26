--创建报表：
use ev_schedule;
create table if not exists ev_schedule.fangmiao_wangpei_lock_duration_distribution
(
	`bz_date` string comment '日期',
	`city_name`   string comment '城市名称',
	`system_type`   string comment '系统类型',
	`operateLock_type` string comment '锁操作类型',
	`deltatime_type` string comment '锁操作时间',
	`operate_times` bigint comment '操作总时间',
	`operate_nums` bigint comment '操作总记录数'
)
partitioned by 
(`pt` string comment 'increment_partition')
;


--全量数据一次性插入：
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=400;
set hive.exec.max.dynamic.partitions=400;

add jar /home/master/platform/env/hiveudf/AppBiHiveUDF-0.0.1-SNAPSHOT.jar;
create temporary function getLongTime as 'com.hellobike.bi.udf.LongTime';

insert overwrite table ev_schedule.fangmiao_wangpei_lock_duration_distribution partition(pt)
select 
	substr(mm.request_time,1,10) bz_date,
	mm.city_name,
	mm.system_type,
	mm.operateLock_type,
	case when mm.operateLockStatus_type='成功' then round(mm.deltatime/1000,0)
	     when mm.operateLockStatus_type='失败' then '失败'
	     else 'unknown'
	end deltatime_type,
	round(sum(deltatime)/1000,2) operate_times,
	count(*) operate_nums,
	mm.pt
from 
(
select 
	aa.orderguid,
	aa.userguid,
	aa.bikeno,
	aa.pt,
	cc.city_name,
	case when aa.systemcode='65' or (aa.systemcode is null and bb.systemcode='65') then '小程序'
	     when aa.systemcode='62' or (aa.systemcode is null and bb.systemcode='62') then 'APP&Android'
	     when aa.systemcode='61' or (aa.systemcode is null and bb.systemcode='61') then 'APP&iOS'
	     else 'unknown'
	end system_type,
	case when aa.operateLockType='OPEN' then '开锁'
	     when aa.operateLockType='CLOSE' then '关锁'
	     when aa.operateLockType='PAUSE' then '临时锁车'
	     when aa.operateLockType='RESUME' then '临时锁车解锁'
	     else 'unknown'
	end operateLock_type,
	case when bb.operateLockStatus='SUCCESS' then '成功'
	     when bb.operateLockStatus='FAILED' then '失败'
	     else 'unknown'
	end operateLockStatus_type,
	aa.createtime request_time,
	bb.createtime response_time,
	default.getLongTime(bb.createtime)-default.getLongTime(aa.createtime) as deltatime
from 
(
select *
from odssub.open_lock_request_time_metrics
where pt>='20191001'

union all

select *
from odssub.close_lock_request_time_metrics
where pt>='20191001'
) aa
left join
(
select *
from odssub.open_lock_response_time_metrics
where pt>='20191001'

union all

select *
from odssub.close_lock_response_time_metrics
where pt>='20191001'
) bb
on aa.orderguid=bb.orderguid and aa.operateLockType=bb.operateLockType
left join
(
select *
from ods.t_city
where pt='${dt}'
) cc
on aa.citycode=cc.city_code
) mm
where mm.operateLock_type in ('开锁','关锁')
  and mm.system_type in ('小程序','APP&Android','APP&iOS')
  and mm.operateLockStatus_type in ('成功','失败')
  and mm.deltatime>0
group by 
	substr(mm.request_time,1,10),
	mm.city_name,
	mm.system_type,
	mm.operateLock_type,
	case when mm.operateLockStatus_type='成功' then round(mm.deltatime/1000,0)
	     when mm.operateLockStatus_type='失败' then '失败'
	     else 'unknown'
	end,
	mm.pt
;

--日增量数据插入：
add jar /home/master/platform/env/hiveudf/AppBiHiveUDF-0.0.1-SNAPSHOT.jar;
create temporary function getLongTime as 'com.hellobike.bi.udf.LongTime';

insert overwrite table ev_schedule.fangmiao_wangpei_lock_duration_distribution partition(pt='${dt}')
select 
	substr(mm.request_time,1,10) bz_date,
	mm.city_name,
	mm.system_type,
	mm.operateLock_type,
	case when mm.operateLockStatus_type='成功' then round(mm.deltatime/1000,0)
	     when mm.operateLockStatus_type='失败' then '失败'
	     else 'unknown'
	end deltatime_type,
	round(sum(deltatime)/1000,2) operate_times,
	count(*) operate_nums
from 
(
select 
	aa.orderguid,
	aa.userguid,
	aa.bikeno,
	aa.pt,
	cc.city_name,
	case when aa.systemcode='65' or (aa.systemcode is null and bb.systemcode='65') then '小程序'
	     when aa.systemcode='62' or (aa.systemcode is null and bb.systemcode='62') then 'APP&Android'
	     when aa.systemcode='61' or (aa.systemcode is null and bb.systemcode='61') then 'APP&iOS'
	     else 'unknown'
	end system_type,
	case when aa.operateLockType='OPEN' then '开锁'
	     when aa.operateLockType='CLOSE' then '关锁'
	     when aa.operateLockType='PAUSE' then '临时锁车'
	     when aa.operateLockType='RESUME' then '临时锁车解锁'
	     else 'unknown'
	end operateLock_type,
	case when bb.operateLockStatus='SUCCESS' then '成功'
	     when bb.operateLockStatus='FAILED' then '失败'
	     else 'unknown'
	end operateLockStatus_type,
	aa.createtime request_time,
	bb.createtime response_time,
	default.getLongTime(bb.createtime)-default.getLongTime(aa.createtime) as deltatime
from 
(
select *
from odssub.open_lock_request_time_metrics
where pt='${dt}'

union all

select *
from odssub.close_lock_request_time_metrics
where pt='${dt}'
) aa
left join
(
select *
from odssub.open_lock_response_time_metrics
where pt='${dt}'

union all

select *
from odssub.close_lock_response_time_metrics
where pt='${dt}'
) bb
on aa.orderguid=bb.orderguid and aa.operateLockType=bb.operateLockType
left join
(
select *
from ods.t_city
where pt='${dt}'
) cc
on aa.citycode=cc.city_code
) mm
where mm.operateLock_type in ('开锁','关锁')
  and mm.system_type in ('小程序','APP&Android','APP&iOS')
  and mm.operateLockStatus_type in ('成功','失败')
  and mm.deltatime>0
group by 
	substr(mm.request_time,1,10),
	mm.city_name,
	mm.system_type,
	mm.operateLock_type,
	case when mm.operateLockStatus_type='成功' then round(mm.deltatime/1000,0)
	     when mm.operateLockStatus_type='失败' then '失败'
	     else 'unknown'
	end
;

