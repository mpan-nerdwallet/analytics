create table dw_workarea.mp_mktg_campaignspend_test (
report_date date NOT NULL encode lzo
,vendor VARCHAR(100) NULL encode lzo
, channel VARCHAR(100) NULL encode lzo
, marketing_vertical VARCHAR(100) NULL encode lzo
, utm_campaign_id VARCHAR(100) NULL encode lzo
, sessions INT NULL encode lzo 
, PV INT NULL encode lzo 
, UV INT NULL encode lzo 
, mPV INT NULL encode lzo 
, mUV INT NULL encode lzo 
, clicks INT NULL encode lzo 
, sponsored_clicks INT NULL encode lzo 
, clickers INT NULL encode lzo 
, mclickers INT NULL encode lzo 
, txns INT NULL encode lzo 
, revenue numeric(38,4) NULL encode lzo
, clicks_ad INT NULL encode lzo
, impressions INT NULL encode lzo 
, spend numeric(38,4) NULL encode lzo
) DISTKEY (utm_campaign_id) SORTKEY (report_date)


grant all on dw_workarea.mp_mktg_campaignspend_test to group grp_ba_users;
grant all on dw_workarea.mp_mktg_campaignspend_test to group grp_power_users;
grant select on dw_workarea.mp_mktg_campaignspend_test to group grp_data_users;


insert into dw_workarea.mp_mktg_campaignspend_test(
select
dw_eff_dt as report_date
--, acct_desc_nm
, INITCAP(cmpgn_spend.vendor) as vendor
, cmpgn_spend.channel
, cmpgn_spend.vertical_nm as marketing_vertical
, utm_campaign_id
, sum(sessions) as sessions
, sum(PV) as PV
, sum(UV) as UV
, sum(mPV) as mPV
, sum(mUV) as mUV
, sum(clicks) as clicks
, sum(sponsored_clicks) as sponsored_clicks
, sum(clickers) as clickers
, sum(mclickers) as mclickers
, sum(txns) as txns
, sum(revenue) as revenue
, sum(clicks_ad) clicks_ad 
, sum(impressions) impressions
, sum(spend) as spend

from 
(select
dw_eff_dt
--,acct_desc_nm
--,src_fact_tbl_nm
, lower(trim(campaign_nm)) as campaign_nm
, case when lower(referrer_nm) in ('facebook','taboola') then 'n/a' else dw_campaign_type_desc end as channel
,  referrer_nm as vendor
, coalesce(v.vertical_nm, 'Unknown Mktg Vertical') as vertical_nm
, sum(clicks_ct) clicks_ad 
, sum(imprsn_ct) impressions
, sum(cost_am) as spend
from mktg_consolidated_campaign_perf_f f
left join dw_report.mktg_vertical_d v
on f.vertical_id = v.vertical_id
 left join mktg_campaign_domain_d cd
 on f.campaign_domain_id = cd.campaign_domain_id 
left join mktg_campaign_type_d ct
--on f.campaign_type_id = ct.campaign_type_id
on f.campaign_type_id = ct.campaign_type_id and f.campaign_domain_id = ct.campaign_domain_id
where dw_eff_dt between '2015-01-01' and (current_date-1)
and f.campaign_domain_id in(101,102,107,111)
group by 1,2,3,4,5)cmpgn_spend
left outer join -- need to roll ba_mktg_summary to campaign level to join to cmpgn
(select
 report_date
   , Vendor
   , lower(trim(utm_campaign_id)) as utm_campaign_id
   , case when lower(vendor) in ('facebook','taboola') then 'n/a' else channel end as channel
   --, entry_vertical
   , sum(sessions) as sessions
   , sum(PV) as PV
   , sum(UV) as UV
   , sum(mPV) as mPV
   , sum(mUV) as mUV
   , sum(clicks) as clicks
   , sum(sponsored_clicks) as sponsored_clicks
   , sum(clickers) as clickers
   , sum(mclickers) as mclickers
   , sum(apps) as apps
   , sum(txns) as txns
   , sum(revenue) as revenue
   , sum(hb_sessions) as hb_sessions
   , sum(b_sessions) as  b_sessions
from dw_ba_report.ba_mktg_summary 
where report_date between '2015-01-01' and (current_date-1)
group by 1,2,3,4)ba

 on ba.report_date = cmpgn_spend.dw_eff_dt
and trim(upper(ba.Vendor)) = trim(upper(cmpgn_spend.vendor))
and trim(upper(ba.utm_campaign_id)) = trim(upper(cmpgn_spend.campaign_nm))
and trim(upper(ba.channel)) = trim(upper(cmpgn_spend.channel)) 
 --where clicks_ad is not null or impressions is not null or spend is not null   
group by 1,2,3,4,5
)
