-- enter Impala

select SAT_AVG,ADM_RATE, TUITIONFEE_IN,AVGFACSAL,RPY_3YR_RT,C150_4_White, C150_4_Black, C150_4_Hisp
from final
where (year = '2013') and (SAT_AVG != 'NULL') and (C150_4_White != 'NULL') and (C150_4_Black != 'NULL') and (C150_4_Hisp != 'NULL') and (RPY_3YR_RT != 'PrivacySuppressed') and (RPY_3YR_RT != 'NULL');

-- run this file with:
-- impala-shell -i localhost -f cluster.sql -o cluster_input --output_file_field_delim=' '







