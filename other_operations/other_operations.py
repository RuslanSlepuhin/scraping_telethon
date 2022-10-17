

from db_operations.scraping_db import DataBaseOperations

def delete_since():
    """
    delete records since time in params in tables in list[]
    """
    for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst', 'mobile', 'qa', 'hr', 'game',
              'ba', 'marketing', 'junior', 'sales_manager']:
        DataBaseOperations(None).delete_data(table_name=i, param="WHERE created_at > '2022-10-17 02:00:00'")

delete_since()