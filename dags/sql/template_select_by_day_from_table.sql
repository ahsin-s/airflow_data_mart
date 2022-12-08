select *
from {{ params.table_schema }}.{{ params.table_name }}
where {{ params.date_column }} = '{{ params.selection_date }}'
order by {{ params.order_columns }}