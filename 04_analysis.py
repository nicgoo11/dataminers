import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)

combined_df_path = 'xxx'

combined_df = pd.read_csv(combined_df_path, sep=',')

# Filtering the dataframe for the months 02-2022 to 12-2022
combined_df['Year-Month'] = combined_df['Year'].astype(str) + "-" + combined_df['Month'].astype(str).str.zfill(2)
filtered_df = combined_df[(combined_df['Year-Month'] >= '2022-02') & (combined_df['Year-Month'] <= '2022-12')]

# Exclude the 'Year-Month' and other non-numeric columns before calculating the correlation matrix
numerical_df = filtered_df.select_dtypes(include=[np.number])

print(numerical_df)

# Calculate the Pearson correlation matrix for the filtered data
correlation_matrix = numerical_df.corr()
correlation_matrix = round(correlation_matrix, 3)

# Print the correlation matrix
print(correlation_matrix)


correlation_matrix.to_excel('correlation_matrix.xlsx')