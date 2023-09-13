
from signalling_tables import table_name_list
from functions.signalling_functions import check_signalling_issues

# Apply the checks from dictionary
for table_name in table_name_list:
    df = check_signalling_issues(table_name)
    
# Write results to table