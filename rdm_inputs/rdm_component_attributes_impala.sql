/****** Script for SelectTopNRows command from SSMS  ******/
SELECT system_id
      ,1 component_type_id
      ,agency_id component_lookup_value
	  ,1 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'sector_id' attribute_name
	  ,7 attribute_type_id
	  ,sector_id attribute_lookup_value
	  ,null attribute_value
  FROM dwh.dim_agency where system_id!=0
  union all
  SELECT system_id
      ,1 component_type_id
      ,agency_id component_lookup_value
	  ,2 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'city_id' attribute_name
	  ,null attribute_type_id
	  ,null attribute_lookup_value
	  ,cast(city_id as string) attribute_value
  FROM dwh.dim_agency where system_id!=0
    union all
  SELECT system_id
      ,2 component_type_id
      ,platform_id component_lookup_value
	  ,1 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'agency_id' attribute_name
	  ,1 attribute_type_id
	  ,agency_id attribute_lookup_value
	  ,null attribute_value
  FROM dwh.dim_platform where system_id!=0
      union all
  SELECT system_id
      ,2 component_type_id
      ,platform_id component_lookup_value
	  ,2 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'c_type_id' attribute_name
	  ,null attribute_type_id
	  ,null attribute_lookup_value
	  ,cast(c_type_id as string) attribute_value
  FROM dwh.dim_platform where system_id!=0
        union all
  SELECT system_id
      ,2 component_type_id
      ,platform_id component_lookup_value
	  ,2 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'main_domain' attribute_name
	  ,null attribute_type_id
	  ,null attribute_lookup_value
	  ,main_domain attribute_value
  FROM dwh.dim_platform where system_id!=0
          union all
  SELECT system_id
      ,3 component_type_id
      ,service_id component_lookup_value
	  ,1 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'platform_id' attribute_name
	  ,2 attribute_type_id
	  ,platform_id attribute_lookup_value
	  ,null attribute_value
  FROM dwh.dim_service where system_id!=0
  union all
  SELECT system_id
      ,3 component_type_id
      ,service_id component_lookup_value
	  ,1 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'agency_id' attribute_name
	  ,1 attribute_type_id
	  ,agency_id attribute_lookup_value
	  ,null attribute_value
  FROM dwh.dim_service where system_id!=0
    union all
  SELECT system_id
      ,3 component_type_id
      ,service_id component_lookup_value
	  ,2 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'service_url' attribute_name
	  ,null attribute_type_id
	  ,null attribute_lookup_value
	  ,service_url attribute_value
  FROM dwh.dim_service where system_id!=0
      union all
  SELECT system_id
      ,4 component_type_id
      ,channel_id component_lookup_value
	  ,1 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'platform_id' attribute_name
	  ,2 attribute_type_id
	  ,platform_id attribute_lookup_value
	  ,null attribute_value
  FROM dwh.dim_platform_channel where system_id!=0
    union all
  SELECT system_id
      ,4 component_type_id
      ,channel_id component_lookup_value
	  ,2 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'url' attribute_name
	  ,null attribute_type_id
	  ,null attribute_lookup_value
	  ,url attribute_value
  FROM dwh.dim_platform_channel where system_id!=0
  union all
  SELECT system_id
      ,5 component_type_id
      ,product_id component_lookup_value
	  ,1 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'platform_id' attribute_name
	  ,2 attribute_type_id
	  ,platform_id attribute_lookup_value
	  ,null attribute_value
  FROM dwh.dim_platform_product where system_id!=0
    union all
  SELECT system_id
      ,6 component_type_id
      ,vendor_id component_lookup_value
	  ,2 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'email' attribute_name
	  ,null attribute_type_id
	  ,null attribute_lookup_value
	  ,email attribute_value
  FROM dwh.dim_vendor where system_id!=0
      union all
  SELECT system_id
      ,6 component_type_id
      ,vendor_id component_lookup_value
	  ,2 attribute_category /*1 for lookup, 2 for vlaue */
	  ,'contact_number' attribute_name
	  ,null attribute_type_id
	  ,null attribute_lookup_value
	  ,contact_number attribute_value
  FROM dwh.dim_vendor where system_id!=0