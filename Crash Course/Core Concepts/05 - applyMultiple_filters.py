# Apply filter on 'first_name' and 'city' columns
df_filter = df.filter((df.first_name.isin('Aldin', 'Valma')) & (df.city.like('%ondon')))
df_filter.show()

# Use 'OR' statement
df_filter = df.filter((df.first_name.isin('Aldin', 'Valma')) | (df.city.like('%ondon')))
df_filter.show()

# Apply filter to get data using '&' instead of 'between'
df_filter = df.filter((df.id > 10) & (df.id < 100))
df_filter.show()
