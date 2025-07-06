/****** rdm_input ******/
SELECT 22 system_id
	  ,1 type_id, 'Agency' type_name
	  ,null parent_type_id
	  ,null parent_lookup_value
	  ,str(agency_id) lookup_value
      ,str(Agency_Id) id
      ,code
      ,NameAr name_ar
      ,NameEn name_en
FROM [qiyas_dwh].[dbo].[DIM_QIYAS_AGENCY] 
where Qiyas_Year=(select max(Qiyas_Year) from [qiyas_dwh].[dbo].[DIM_QIYAS_AGENCY])
union
SELECT system_id
	  ,1 type_id, 'Agency' type_name
	  ,null parent_type_id
	  ,null parent_lookup_value
	  ,agency_id lookup_value
      ,sys_id id
      ,code
      ,name_ar
      ,name_en
FROM DGA_DWH.dbo.dim_agency where system_id!=0
union all
SELECT system_id
	  ,2 type_id, 'Platform' type_name
	  ,1 parent_type_id
	  ,agency_id parent_lookup_value
	  ,platform_id lookup_value
      ,sys_id id
      ,code
      ,name_ar
      ,name_en
FROM DGA_DWH.dbo.dim_platform where system_id!=0
union all
SELECT system_id
	  ,3 type_id, 'Service' type_name
	  ,case when platform_id is null then 1 else 2 end parent_type_id
	  ,case when platform_id is null then agency_id else platform_id end parent_lookup_value
	  ,service_id lookup_value
      ,sys_id id
      ,code
      ,name_ar
      ,name_en
FROM DGA_DWH.dbo.dim_service where system_id!=0
union all
SELECT system_id
	  ,4 type_id, 'Platform Channel' type_name
	  ,2 parent_type_id
	  ,platform_id  parent_lookup_value
	  ,channel_id lookup_value
      ,sys_id id
      ,code
      ,name_ar
      ,name_en
FROM DGA_DWH.dbo.dim_platform_channel where system_id!=0
union all
SELECT system_id
	  ,5 type_id, 'Platform Product' type_name
	  ,2 parent_type_id
	  ,platform_id  parent_lookup_value
	  ,product_id lookup_value
      ,sys_id id
      ,code
      ,name_ar
      ,name_en
FROM DGA_DWH.dbo.dim_platform_product where system_id!=0
union all
SELECT system_id
	  ,6 type_id, 'Vendor' type_name
	  ,null parent_type_id
	  ,null parent_lookup_value
	  ,vendor_id lookup_value
      ,null id
      ,null code
      ,name_ar
      ,name_en
FROM DGA_DWH.dbo.dim_vendor where system_id!=0
union all
SELECT system_id
	  ,7 type_id, 'Sector' type_name
	  ,null parent_type_id
	  ,null parent_lookup_value
	  ,sector_id lookup_value
      ,null id
      ,null code
      ,name_ar
      ,name_en
FROM DGA_DWH.dbo.dim_sector where system_id!=0