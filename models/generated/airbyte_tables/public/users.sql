{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "public",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='users_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('users_ab3') }}

-- update value of column in the table
--{{ config(
   --post_hook="UPDATE {{ this }} SET user_mobile='93508063'"
--) }}

-- add new column to the table
--{{ config(
  -- post_hook="ALTER TABLE {{ this }} ADD isAdded bool default false"
--) }}

-- remove column from the table
{{ config(
   post_hook="ALTER TABLE {{ this }} DROP COLUMN _airbyte_ab_id"
) }}
{{ config(
   post_hook="ALTER TABLE {{ this }} DROP COLUMN _airbyte_emitted_at"
) }}
{{ config(
   post_hook="ALTER TABLE {{ this }} DROP COLUMN _airbyte_normalized_at"
) }}
{{ config(
   post_hook="ALTER TABLE {{ this }} DROP COLUMN _airbyte_users_hashid"
) }}

-- rename the name of the table
--{{ config(
   --post_hook="ALTER TABLE {{ this }} RENAME TO my_users"
--) }}


-- Drop table from database
{{ config(
   post_hook="DROP TABLE {{ this.database }}.{{ this.schema }}._airbyte_raw_users"
) }}

select
    {{ adapter.quote('id') }},
    first_name,
    last_name,
    full_name,
    --key_cloak_id,
    --branch_name,
    mobile,
    --bank_name,
    --omsb_id,
    --account_no,
    email,
    date_of_birth,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_users_hashid
from {{ ref('users_ab3') }}
-- users from {{ source('public', '_airbyte_raw_users') }}
where 1 = 1

