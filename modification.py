import pandas as pd
import numpy as np

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from dateutil.parser import parse
from datetime import datetime

import datetime

pd.options.mode.chained_assignment = None  # default='warn'

# read base data to dataframe
input_data = pd.read_csv("Base data.csv")

# read correction data to another dataframe
modify_data = pd.read_excel("Correction data.xlsx")

# combine both the dataframe based on member ID and relationship
# only data which exists on correction data will be collected
# both core data and our correction data will be joined -- easy for comparison sake

# Convert strings in Remarks column to upper case for consistency
modify_data["Remarks"] = modify_data["Remarks"].str.upper()

# Convert "in-laws" string in relationship column to Title case for proper matching
modify_data["Relationship"] = modify_data["Relationship"].str.replace("-in-","-In-")
modify_data["Relationship"] = modify_data["Relationship"].str.replace("law","Law")

# Combine brokers data and core data based on two criteria - Employee ID and Relationship
df_combine = input_data.merge(modify_data,left_on=("MEMBER ID","RELATIONSHIP"),right_on=("Emp id","Relationship"),how="right")

# Drop the rows which are present in brokers' data but not in core data
# This dataframe will be exported to an excel file -- Please go through the excel and manually confirm the employee ID is not present in core.
# If the employee ID or Relationship or both is incorrect in brokers' data, the script will not match the rows.
# We have to manually edit the brokers' data and re-run the script for perfect results.

df_notdropped = df_combine.dropna(subset=['NAME'])

# running fuzzy match function for all the present rows
# the purpose of running fuzzy match is to compare the names present in core data and brokers' data
# if the fuzzy match score is less than 95, we assume that the rows are not to be presented for modification
# please do check the exported dataframe if the script has not made any drastic errors in removing the rows

df_notdropped['partial_ratio'] = df_notdropped.apply(lambda x: fuzz.partial_ratio(x['NAME'], x['Name']), axis=1)

df_dropped = df_combine.drop(df_notdropped.index)
df_dropped.to_csv("output_not_available_in_core.csv",index=False)

# dropping all the rows where the fuzzy match score is less than 95

df_match = df_notdropped[df_notdropped['partial_ratio']> 95]
df_not_match = df_notdropped[df_notdropped['partial_ratio']< 96]
df_not_match.to_csv("output_fuzzy_not_matching.csv",index=False)

# First stage of the script ends here. We are exporting the dataframe at this stage to an excel file for reference:
df_match.to_csv("output_step1.csv",index=False)

# convert the status column to "M" for modification
df_notdropped["STATUS"] = "M"

# reading output_step1.csv from first stage to dataframe
df_corrections = pd.read_csv("output_step1.csv")

# remove left side space string -- stripping whitespaces at the start of the name or other values
df_corrections["Correct Name"] = df_corrections["Correct Name"].str.lstrip()

# Making a copy of name that exists in core and corrections to be made to new columns "Old name" and "preserve" for reference
df_corrections["Old name"] = df_corrections["NAME"]
df_corrections["preserve"] = df_corrections["Correct Name"]

# Replacing / used as a delimiter into & delimiter
# If there are other kinds of delimiters used, please use the below line as format and add them as new delimiters
df_corrections["Correct Name"] = df_corrections["Correct Name"].str.replace(" / "," & ")

# Splitting Corrections column into two (Correct Name and DOB) using & as delimiter
# Hopefully brokers' data is in the format NAME & DOB.
# Otherwise, manually convert brokers' data in that format. :(

df_corrections[["Correct Name","DOB"]] = df_corrections["Correct Name"].str.split(" & ",expand=True)

# If corrections remarks states "DOB Correction", copy the DOB value to newly created DOB column
df_corrections.loc[df_corrections.Remarks == "DOB CORRECTION", "DOB"] = df_corrections["Correct Name"]

# If the correction remarks states *NAME*, copy the Correct Name column to core NAME column
df_corrections.loc[df_corrections["Remarks"].str.contains("NAME"),"NAME"] = df_corrections["Correct Name"]

# Convert the DOB string to datetime format for further processing
df_corrections["DOB_inferred"] = pd.to_datetime(df_corrections["DOB"],dayfirst=True)

# function created for calculating Age from DOB and today's value (today is whenever the script is run)

def from_dob_to_age(born):
    today = datetime.date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

# New age column is created using the from_dob_to_age function applied on DOB_inferred column
df_corrections['New Age'] = df_corrections['DOB_inferred'].apply(lambda x: from_dob_to_age(x))

# Comparing old name copied earlier with the current modified NAME column to see if name correction has been executed

df_corrections['name correction'] = df_corrections.apply(lambda row: row['Old name'] == row['NAME'], axis=1).astype(int)
df_corrections['name correction'] = np.where(df_corrections['name correction'], 'Name not corrected', 'Name corrected')

# Copying AGE column from core data to "Old age" column for reference
df_corrections["Old age"] = df_corrections['AGE']

# Calculating difference between existing age and the new age calculated
df_corrections['Age diff'] = df_corrections['Old age'] - df_corrections['New Age']

# If the Corrections remarks mention the string "DOB", copy the new age calculated to the core AGE column
df_corrections.loc[df_corrections["Remarks"].str.contains("DOB"),"AGE"] = df_corrections["New Age"]

# Sort the final dataframe descending order on Remarks column
# We have made corrections if the remarks column have the following exact strings alone:
# 1. NAME CORRECTION
# 2. DOB CORRECTION
# 3. NAME & DOB CORRECTION

df_corrections = df_corrections.sort_values(by=["Remarks"],ascending=False)

# Export the output at this stage to "output_step2" for reference
df_corrections.to_csv("output_step2.csv",index=False)

# If there is no age difference between old age and new age calculated, remove them from our final dataframe
df_dob = df_corrections.loc[df_corrections.Remarks == "DOB CORRECTION"]
df_noagediff = df_dob[df_dob['Age diff'] == 0]
df_agediff = df_corrections.drop(df_noagediff.index)

# Export "output_final" for uploading and "no diff" file for reference
df_agediff.to_csv("output_final.csv",index=False)
df_noagediff.to_csv("output_no_age_diff.csv",index=False)

# printing input summary for reference

input_summary = pd.pivot_table(modify_data,index='Remarks',values='Emp id',aggfunc='count')
input_summary.loc['Total'] = input_summary.sum(numeric_only=True,axis=0)

print("=============================\nINPUT COUNT\n",input_summary)

# printing final output summary for reference
output_summary = pd.pivot_table(df_agediff,index='Remarks', values= 'Emp id', aggfunc='count')
output_summary.loc['Total'] = output_summary.sum(numeric_only=True,axis=0)
print("=============================\nFINAL OUTPUT COUNT\n",output_summary)

print("Name corrections, Name & DOB corrections and DOB corrections have been made. Please make all other types of corrections manually. :(")

# printing summary of entries where there is no age difference between core data and brokers' data
if not df_noagediff.empty:
    output_summary_noagediff = pd.pivot_table(df_noagediff,index='Remarks', values= 'Emp id', aggfunc='count')
    output_summary_noagediff.loc['Total'] = output_summary_noagediff.sum(numeric_only=True,axis=0)
    print("=============================\nNO AGE DIFFERENCE COUNT\n",output_summary_noagediff)
else:
    print("All DOB correction entries were made. There were no entries where difference between existing age and newly calculated age was zero.")

# not available in core here

if not df_dropped.empty:

    output_summary_dropped = pd.pivot_table(df_dropped,index='Remarks', values= 'Emp id', aggfunc='count')
    output_summary_dropped.loc['Total'] = output_summary_dropped.sum(numeric_only=True,axis=0)
    print("=============================\nNOT AVAILABLE IN CORE COUNT\n",output_summary_dropped)
    print("Please manually check if the data provided by brokers is accurate. \nIf employee ID and relationship (both) is inaccurate, they won't be matched. \nMake manual correction in brokers' data and re-run the script for correct results.")

else:
    print("All input entries were available in core data.")

# fuzzy not matching here

if not df_not_match.empty:

    output_summary_fuzzy_nomatch = pd.pivot_table(df_not_match,index='Remarks', values= 'Emp id', aggfunc='count')
    output_summary_fuzzy_nomatch.loc['Total'] = output_summary_fuzzy_nomatch.sum(numeric_only=True,axis=0)
    print("=============================\nFUZZY NO MATCH -NAME NOT MATCHING IN CORE DATA\n",output_summary_fuzzy_nomatch)
    print("For employees with multiple sons and/or daughters, if there is correction to be made for one of the son/daughter, \nthe other son/daughter entry will be removed based on fuzzy match. \nPlease have a look into the file to confirm the fuzzy match has been successful. \nIf script has incorrectly removed the entry, please manually modify the \"Name\" column in brokers' data and rerun the script for correct results.")
else:
    print("All input entries were matched based on fuzzy data.")
