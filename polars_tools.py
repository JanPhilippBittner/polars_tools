import polars as pl 



def down_cast_numeric_cols(lazy_frame) -> "LazyFrame":
    # Define possible data types for downcasting
    # From largest to smalles creating unnecessary steps
    #dt_types = [ pl.Float64 , pl.Float32 , pl.Int64 , pl.Int32 , pl.Int16, pl.Int8]
    
    # From smallest to largest to minimze unnecessary steps
    dt_types = [pl.Int8 , pl.Int16 , pl.Int32 , pl.Int64 , pl.Float32 , pl.Float64]

    # Select numeric columns
    numeric_col_list = lazy_frame.select(pl.col(dt_types)).collect_schema().names()


    # Try to downcast each numeric column to a smaller type
    for column in numeric_col_list:
        original_expr = pl.col(column)  # Keep track of the original column
        for dt_type in dt_types:
            try:
                # Test if the cast is valid
                expr = original_expr.cast(dt_type)
                lf_test = lazy_frame.with_columns(expr)  # Create a new LazyFrame with the casted column
                # Collect to validate if the cast succeeded
                lf_test.collect()
            except Exception:
                print(f"Couldn't cast {column} to {dt_type}")
                break
            else:
                lazy_frame = lazy_frame.with_columns(expr)
                print(f"Casting {column} to {dt_type}")
    
    # Final LazyFrame with downcasted columns
    lazy_frame = lazy_frame.collect().lazy()
    return lazy_frame