def rentals_month(bd, month, year):
    import os
    from dotenv import load_dotenv
    import pandas as pd
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus

    # Load the .env file with the db_pass password
    load_dotenv()

    # Get the password securely
    db_pass = os.getenv("db_pass")
    if not db_pass:
        raise ValueError("The 'db_pass' environment variable is not defined in the .env file.")

    # Database name
    #bd = "ucl21_22"

    # Escape special characters in the password and the database name
    escaped_db_pass = quote_plus(db_pass)  # Escape password
    escaped_bd = quote_plus(bd)            # Escape database name

    # Create the connection string
    connection_string = f'mysql+pymysql://root:{escaped_db_pass}@localhost/{escaped_bd}'
    engine = create_engine(connection_string)

    # Leer la consulta SQL en un DataFrame
    consulta = f"""
    SELECT *,
        YEAR(sr.rental_date) as year_date, 
        MONTH(sr.rental_date) as month_date,
        COUNT(sr.customer_id) as total_bymonth
    FROM sakila.rental as sr
    WHERE MONTH(sr.rental_date)={month} and YEAR(sr.rental_date)={year}
    GROUP BY sr.rental_id;
        """ 
    df = pd.read_sql(consulta, con=engine)
    return df


def rental_count_month(df, month, year):
    import pandas as pd
    """
    This function takes the rental DataFrame, the month, and the year as input,
    and returns a new DataFrame with the number of rentals for each customer_id
    during the selected month and year.
    
    Args:
    - df: DataFrame with rental data (output from rentals_month).
    - month: The month (integer) for which to count rentals.
    - year: The year (integer) for which to count rentals.

    Returns:
    - A new DataFrame with the customer_id and the corresponding number of rentals for the month and year.
    """
    # Ensure the month and year are integers
    month = int(month)
    year = int(year)
    
    # Filter the DataFrame for the selected month and year
    filtered_df = df[(df['month_date'] == month) & (df['year_date'] == year)]
    
    # Count the number of rentals by customer_id
    rentals_by_customer = filtered_df.groupby('customer_id').size().reset_index(name=f'rentals_{month:02d}_{year}')
    
    return rentals_by_customer


def compare_rentals(df1, df2, month_year_1, month_year_2):
    import pandas as pd
    """
    Compares two DataFrames containing the number of rentals made by each customer in different months and years,
    and returns a combined DataFrame with a 'difference' column.
    
    Args:
    - df1: First DataFrame with customer_id and rental count for the first month/year.
    - df2: Second DataFrame with customer_id and rental count for the second month/year.
    - month_year_1: A string or identifier for the first month/year (e.g., 'rentals_05_2005').
    - month_year_2: A string or identifier for the second month/year (e.g., 'rentals_06_2005').
    
    Returns:
    - A DataFrame with customer_id, rental counts for both months, and the 'difference' column.
    """
    
    # Merge the two DataFrames on 'customer_id'
    combined_df = pd.merge(df1[['customer_id', month_year_1]], df2[['customer_id', month_year_2]], on='customer_id', how='outer')
    
    # Fill missing values with 0
    combined_df[month_year_1] = combined_df[month_year_1].fillna(0)
    combined_df[month_year_2] = combined_df[month_year_2].fillna(0)
    
    # Calculate the difference
    combined_df['difference'] = combined_df[month_year_2] - combined_df[month_year_1]
    
    return combined_df