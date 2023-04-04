{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('users_ab1') }}
select
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('id') }},
    cast(email as {{ dbt_utils.type_string() }}) as email,
    cast(mobile as {{ dbt_utils.type_string() }}) as mobile,
    cast(omsb_id as {{ dbt_utils.type_string() }}) as omsb_id,
    cast(bank_name as {{ dbt_utils.type_string() }}) as bank_name,
    cast(full_name as {{ dbt_utils.type_string() }}) as full_name,
    cast(last_name as {{ dbt_utils.type_string() }}) as last_name,
    cast(account_no as {{ dbt_utils.type_string() }}) as account_no,
    cast(first_name as {{ dbt_utils.type_string() }}) as first_name,
    cast(branch_name as {{ dbt_utils.type_string() }}) as branch_name,
    cast(key_cloak_id as {{ dbt_utils.type_string() }}) as key_cloak_id,
    cast({{ empty_string_to_null('date_of_birth') }} as {{ type_timestamp_with_timezone() }}) as date_of_birth,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('users_ab1') }}
-- users
where 1 = 1

