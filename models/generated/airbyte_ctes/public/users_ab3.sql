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
        adapter.quote('id'),
        'email',
        'mobile',
        'omsb_id',
        'bank_name',
        'full_name',
        'last_name',
        'account_no',
        'first_name',
        'branch_name',
        'key_cloak_id',
        'date_of_birth',
    ]) }} as _airbyte_users_hashid,
    tmp.*
from {{ ref('users_ab2') }} tmp
-- users
where 1 = 1

