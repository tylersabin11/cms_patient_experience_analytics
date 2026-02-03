select
    *
from {{ source('my_datasets', 'raw_application_data') }}