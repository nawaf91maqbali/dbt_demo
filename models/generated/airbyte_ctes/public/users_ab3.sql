{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('users_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'full_name',
        'key_cloak_id',
        'date_of_birth',
        'branch_name',
        'mobile',
        'bank_name',
        'last_name',
        adapter.quote('id'),
        'omsb_id',
        'account_no',
        'first_name',
        'email',
    ]) }} as _airbyte_users_hashid,
    tmp.*
from {{ ref('users_ab2') }} tmp
-- users
where 1 = 1

