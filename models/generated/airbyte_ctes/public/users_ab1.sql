{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_users') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['email'], ['email']) }} as email,
    {{ json_extract_scalar('_airbyte_data', ['mobile'], ['mobile']) }} as mobile,
    {{ json_extract_scalar('_airbyte_data', ['omsb_id'], ['omsb_id']) }} as omsb_id,
    {{ json_extract_scalar('_airbyte_data', ['bank_name'], ['bank_name']) }} as bank_name,
    {{ json_extract_scalar('_airbyte_data', ['full_name'], ['full_name']) }} as full_name,
    {{ json_extract_scalar('_airbyte_data', ['last_name'], ['last_name']) }} as last_name,
    {{ json_extract_scalar('_airbyte_data', ['account_no'], ['account_no']) }} as account_no,
    {{ json_extract_scalar('_airbyte_data', ['first_name'], ['first_name']) }} as first_name,
    {{ json_extract_scalar('_airbyte_data', ['branch_name'], ['branch_name']) }} as branch_name,
    {{ json_extract_scalar('_airbyte_data', ['key_cloak_id'], ['key_cloak_id']) }} as key_cloak_id,
    {{ json_extract_scalar('_airbyte_data', ['date_of_birth'], ['date_of_birth']) }} as date_of_birth,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_users') }} as table_alias
-- users
where 1 = 1

