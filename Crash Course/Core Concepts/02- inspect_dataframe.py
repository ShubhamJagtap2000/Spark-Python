df.head(n)                        # Gives you the first 'n' rows of a spark dataframe

df.describe().show()              # Gives you statistical description of a dataframe(count, min, max, avg, stddev, etc)

df.columns                        # Columns in a dataframe

df.count()                        # Total no. of rows in a dataframe

df.distinct().count()             # No. of distinct records in your dataframe
