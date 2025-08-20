import sqlite3
import pandas as pd
import os
import sys

class SalesAnalyzer:
    """Complete sales analysis solution for Company XYZ assignment."""

    def __init__(self, db_name='db_file_sqlite.db'):
        """Initialize the analyzer with database name."""
        self.db_name = db_name
        self.output_file = 'output.csv'
    
    def solution_pure_sql(self):
        """Solution 1: Pure SQL approach"""
        print("Running Solution 1: Pure SQL Approach")

        try:
            conn = sqlite3.connect(self.db_name)

            sql_query = """SELECT
            T1.customer_id AS Customer,
            T2.age AS Age,
            T4.item_name AS Item,
            SUM(T3.quantity) AS Quantity
            FROM Sales AS T1
            JOIN Customers AS T2
                ON T1.customer_id = T2.customer_id
            JOIN Orders AS T3
                ON T1.sales_id = T3.sales_id
            JOIN Items AS T4
                ON T3.item_id = T4.item_id
            WHERE
                T2.age BETWEEN 18 AND 35
                AND T3.quantity IS NOT NULL
            GROUP BY
                Customer, Item
            HAVING
            SUM(T3.quantity) > 0
            """

            df = pd.read_sql_query(sql_query, conn)
            conn.close()

            print(f"SQL query executed - {len(df)} records found")
            return df

        except Exception as e:
            print(f"Error in SQL solution: {e}")
            return None

    def solution_pandas(self):
        """Solution 2: Pandas approach"""
        print("Running Solution 2: Pandas Approach")

       try:
        # 1. Connect to the SQLite3 database
        conn = sqlite3.connect(db_name)
        print(f"Connected to database: {db_name}")

        # Load tables into Pandas DataFrames
        df_sales = pd.read_sql_query("SELECT * FROM Sales;", conn)
        df_customers = pd.read_sql_query("SELECT * FROM Customers;", conn)
        df_orders = pd.read_sql_query("SELECT * FROM Orders;", conn)
        df_items = pd.read_sql_query("SELECT * FROM Items;", conn)
        
        # 2. Extract the total quantities using Pandas
        # Merge Sales and Customer tables to get age information
        df_merged = pd.merge(df_sales, df_customers, on='customer_id')

        # Filter for customers aged 18-35
        df_filtered = df_merged[df_merged['age'].between(18, 35)]

        # Merge with Orders and Items tables to get quantity and item name
        df_final = pd.merge(df_filtered, df_orders, on='sales_id')
        df_final = pd.merge(df_final, df_items, on='item_id')

        # Drop rows where quantity is NULL
        df_final.dropna(subset=['quantity'], inplace=True)

        # Convert quantity to integer since no decimal points are allowed
        df_final['quantity'] = df_final['quantity'].astype(int)

        # Group by customer_id, age, and item_name and sum the quantities
        result_df = df_final.groupby(['customer_id', 'age', 'item_name'])['quantity'].sum().reset_index()

        # Rename columns to match the required output format
        result_df.columns = ['Customer', 'Age', 'Item', 'Quantity']

        # Exclude items with a total quantity of 0
        result_df = result_df[result_df['Quantity'] > 0]
        
        print("Data processed with Pandas successfully.")

        # 3. Store the query result to a CSV file
        result_df.to_csv(output_file, sep=';', index=False)
        print(f"Results stored to CSV file: {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

    def save_to_csv(self, df):
        """Save DataFrame to CSV with semicolon delimiter"""
        if df is None or df.empty:
            print("No data to save")
            return False

        try:
            df.to_csv(self.output_file, sep=';', index=False)
            print(f"Results saved to: {self.output_file}")
            return True
        except Exception as e:
            print(f"Error saving CSV: {e}")
            return False

    def display_results(self, df, method_name):
        """Display results"""
        if df is None or df.empty:
            print(f"No results for {method_name}")
            return

        print(f"{method_name} Results:")
        print(df.to_string(index=False))
        print(f"Total records: {len(df)}")

    def run_complete_analysis(self):
        """Run the complete analysis workflow"""
        print("Company XYZ Sales Analysis - Assignment Solution")

        # Create database if it doesn't exist
        if not os.path.exists(self.db_name):
            self.create_sample_database()

        # Run both solutions
        sql_results = self.solution_pure_sql()
		print("SQL Solution run successfully")
        pandas_results = self.solution_pandas()
		print("Pandas Solution run successfully")

        # Display results
        self.display_results(sql_results, "Pure SQL")
        self.display_results(pandas_results, "Pandas")

        # Validate consistency
        if sql_results is not None and pandas_results is not None:
            sql_sorted = sql_results.sort_values(['Customer', 'Item']).reset_index(drop=True)
            pandas_sorted = pandas_results.sort_values(['Customer', 'Item']).reset_index(drop=True)

            if sql_sorted.equals(pandas_sorted):
                print("Validation: Both methods produce identical results!")
            else:
                print("Validation: Methods produce different results!")

        # Save to CSV
        if sql_results is not None:
            self.save_to_csv(sql_results)

            # Show CSV content
            print(f"CSV Output ({self.output_file}):")
            try:
                with open(self.output_file, 'r') as f:
                    print(f.read())
            except:
                print("Could not read output file")

        return sql_results

def main():
    analyzer = SalesAnalyzer()
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main()
