# Select only the first and last name from the data
df_select = df.select("fist_name", "last_name")
df_select.show()

# Renaming a column
df_renamed = df.withColumnRenamed('first_name', 'fn')
df_renamed.show()

# Only look for a specific first_name = 'Alvera' in the data
df_filter = df.filter((df.first_name.like("%lver")))
df_filter.show()

df_filter = df.filter((df.first_name == 'Alvera'))
df_filter.show()

# Filter only those records where the first_name ends with 'din'
df_filter1 = df.filter((df.first_name.endswith('din')))
df_filter.show()

df_filter1 = df.filter((df.first_name.like('%din')))
df_filter1.show()

# Filter only those records where first_name starts with 'Alv'
df_filter2 = df.filter((df.first_name.startswith('Alv')))
df_filter2.show()

# Filter only those records where the 'ID' in the dataframe is between 1 and 5
df_filter3 = df.filter((df.id.between(1, 5)))
df_filter3.show()

# Filter only those records where the first_name is in a list of names mentioned
df_filter4 = df.filter((df.first_name.isin('Alvera', 'Aldin')))
df_filter4.show()

# Selecting data with only first 5 letters of the first_name column in the dataframe using substring method
df_substr = df.select(df.first_name, df.first_name.substr(1, 5).alias('name'))
df_substr.show()



