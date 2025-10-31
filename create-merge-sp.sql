-- Python Stored Procedure for Stage to Table MERGE Operation
-- Handles billions of records with proper staging and verification
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE PROCEDURE SNOWFLAKE_LEARNING_DB.PUBLIC.stage_to_table_merge(
    stage_name STRING,
    target_table_name STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
import uuid
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

def generate_staging_table_name(target_table_name: str) -> str:
    """
    Generate a unique staging table name with GUID.
    
    Args:
        target_table_name: Name of the target table
        
    Returns:
        String in format STG_{table_name}_{guid}
    """
    guid = str(uuid.uuid4()).replace('-', '_')
    staging_name = f"STG_{target_table_name}_{guid}"
    return staging_name


def create_staging_table(session: Session, staging_table_name: str, target_table_name: str) -> dict:
    """
    Create a staging table with the same structure as target table.
    
    Args:
        session: Snowflake session object
        staging_table_name: Name for the new staging table
        target_table_name: Name of the target table to copy structure from
        
    Returns:
        dict with status and message
    """
    try:
        # Create temporary staging table with same structure as target
        create_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {staging_table_name} 
            LIKE {target_table_name}
        """
        session.sql(create_sql).collect()
        
        return {
            'status': 'success',
            'message': f'Staging table {staging_table_name} created successfully'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to create staging table: {str(e)}'
        }


def copy_from_stage_to_staging(session: Session, stage_name: str, staging_table_name: str) -> dict:
    """
    Copy data from stage to staging table.
    
    Args:
        session: Snowflake session object
        stage_name: Name of the stage (with @ prefix)
        staging_table_name: Name of the staging table
        
    Returns:
        dict with status, message, and rows_loaded count
    """
    try:
        # Ensure stage name has @ prefix
        if not stage_name.startswith('@'):
            stage_name = f'@{stage_name}'
        
        # Copy data from stage to staging table
        copy_sql = f"""
            COPY INTO {staging_table_name}
            FROM {stage_name}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                TRIM_SPACE = TRUE
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            )
            ON_ERROR = 'CONTINUE'
            FORCE = TRUE
        """
        
        result = session.sql(copy_sql).collect()
        
        # Extract rows loaded from result
        rows_loaded = 0
        if result and len(result) > 0:
            # COPY INTO returns rows with file name, status, rows_parsed, rows_loaded, etc.
            for row in result:
                if hasattr(row, 'ROWS_LOADED'):
                    rows_loaded += row.ROWS_LOADED
                elif hasattr(row, 'rows_loaded'):
                    rows_loaded += row.rows_loaded
        
        return {
            'status': 'success',
            'message': f'Data copied from stage to staging table',
            'rows_loaded': rows_loaded
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to copy from stage: {str(e)}',
            'rows_loaded': 0
        }


def verify_record_count(session: Session, staging_table_name: str, expected_count: int = None) -> dict:
    """
    Verify record count in staging table.
    
    Args:
        session: Snowflake session object
        staging_table_name: Name of the staging table
        expected_count: Expected number of records (optional)
        
    Returns:
        dict with status, message, and actual_count
    """
    try:
        # Count records in staging table
        count_sql = f"SELECT COUNT(*) as record_count FROM {staging_table_name}"
        result = session.sql(count_sql).collect()
        
        actual_count = result[0]['RECORD_COUNT'] if result else 0
        
        message = f'Staging table contains {actual_count} records'
        if expected_count is not None:
            if actual_count == expected_count:
                message += f' (matches expected count of {expected_count})'
            else:
                message += f' (WARNING: expected {expected_count}, difference: {actual_count - expected_count})'
        
        return {
            'status': 'success',
            'message': message,
            'actual_count': actual_count
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to verify record count: {str(e)}',
            'actual_count': 0
        }


def get_primary_key_columns(session: Session, target_table_name: str) -> dict:
    """
    Retrieve primary key columns from the target table.
    
    Args:
        session: Snowflake session object
        target_table_name: Name of the target table
        
    Returns:
        dict with status, message, and pk_columns list
    """
    try:
        # Query to get primary key constraints
        # First, try to get table schema info
        show_pk_sql = f"SHOW PRIMARY KEYS IN TABLE {target_table_name}"
        
        try:
            result = session.sql(show_pk_sql).collect()
            
            if result and len(result) > 0:
                # Extract column names from primary key constraint
                pk_cols = [row['column_name'] for row in result]
                
                return {
                    'status': 'success',
                    'message': f'Found primary key columns: {", ".join(pk_cols)}',
                    'pk_columns': pk_cols
                }
            else:
                # No primary key defined, try to find unique key or use first column
                return {
                    'status': 'warning',
                    'message': 'No primary key defined on table',
                    'pk_columns': []
                }
        except Exception as pk_error:
            # If SHOW PRIMARY KEYS fails, try alternative approach
            # Get columns marked as primary key from DESCRIBE
            describe_sql = f"DESCRIBE TABLE {target_table_name}"
            columns_result = session.sql(describe_sql).collect()
            
            # Look for columns with 'primary key' in kind or is_primary flag
            pk_cols = []
            for row in columns_result:
                # Check various possible column names for primary key info
                kind = str(row['kind']).upper() if 'kind' in row else ''
                
                # Some Snowflake versions include primary key info in 'kind' column
                if 'PRIMARY KEY' in kind:
                    pk_cols.append(row['name'])
            
            if pk_cols:
                return {
                    'status': 'success',
                    'message': f'Found primary key columns: {", ".join(pk_cols)}',
                    'pk_columns': pk_cols
                }
            else:
                return {
                    'status': 'warning',
                    'message': 'No primary key found, will attempt to use UNIQUE constraints or first column',
                    'pk_columns': []
                }
                
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to retrieve primary key columns: {str(e)}',
            'pk_columns': []
        }


def get_unique_key_columns(session: Session, target_table_name: str) -> dict:
    """
    Retrieve unique key columns as fallback if no primary key exists.
    
    Args:
        session: Snowflake session object
        target_table_name: Name of the target table
        
    Returns:
        dict with status, message, and unique_columns list
    """
    try:
        # Try to get unique keys
        show_unique_sql = f"SHOW UNIQUE KEYS IN TABLE {target_table_name}"
        
        try:
            result = session.sql(show_unique_sql).collect()
            
            if result and len(result) > 0:
                # Get the first unique constraint columns
                unique_cols = [row['column_name'] for row in result]
                
                return {
                    'status': 'success',
                    'message': f'Found unique key columns: {", ".join(unique_cols)}',
                    'unique_columns': unique_cols
                }
            else:
                return {
                    'status': 'warning',
                    'message': 'No unique keys defined',
                    'unique_columns': []
                }
        except:
            return {
                'status': 'warning',
                'message': 'Could not retrieve unique keys',
                'unique_columns': []
            }
            
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to retrieve unique keys: {str(e)}',
            'unique_columns': []
        }


def perform_merge(session: Session, staging_table_name: str, target_table_name: str, pk_cols: list) -> dict:
    """
    Perform MERGE operation from staging table to target table.
    
    Args:
        session: Snowflake session object
        staging_table_name: Name of the staging table
        target_table_name: Name of the target table
        pk_cols: List of primary key column names
        
    Returns:
        dict with status, message, and merge statistics
    """
    try:
        # Validate that we have primary key columns
        if not pk_cols or len(pk_cols) == 0:
            return {
                'status': 'error',
                'message': 'No primary key columns provided for MERGE operation',
                'rows_inserted': 0
            }
        
        # Build JOIN condition for MERGE
        join_conditions = ' AND '.join([f't.{col} = s.{col}' for col in pk_cols])
        
        # Get all columns from target table
        describe_sql = f"DESCRIBE TABLE {target_table_name}"
        columns_result = session.sql(describe_sql).collect()
        all_columns = [row['name'] for row in columns_result]
        
        # Build INSERT columns and VALUES
        insert_columns = ', '.join(all_columns)
        insert_values = ', '.join([f's.{col}' for col in all_columns])
        
        # Perform MERGE
        merge_sql = f"""
            MERGE INTO {target_table_name} t
            USING {staging_table_name} s
            ON {join_conditions}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
        """
        
        result = session.sql(merge_sql).collect()
        
        # Extract merge statistics
        rows_inserted = 0
        if result and len(result) > 0:
            row = result[0]
            if hasattr(row, 'number of rows inserted'):
                rows_inserted = row['number of rows inserted']
            elif hasattr(row, 'NUMBER OF ROWS INSERTED'):
                rows_inserted = row['NUMBER OF ROWS INSERTED']
        
        return {
            'status': 'success',
            'message': f'MERGE completed successfully. Rows inserted: {rows_inserted}',
            'rows_inserted': rows_inserted
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to perform merge: {str(e)}',
            'rows_inserted': 0
        }


def cleanup_staging_table(session: Session, staging_table_name: str) -> dict:
    """
    Drop the staging table to clean up.
    
    Args:
        session: Snowflake session object
        staging_table_name: Name of the staging table to drop
        
    Returns:
        dict with status and message
    """
    try:
        drop_sql = f"DROP TABLE IF EXISTS {staging_table_name}"
        session.sql(drop_sql).collect()
        
        return {
            'status': 'success',
            'message': f'Staging table {staging_table_name} dropped successfully'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to cleanup staging table: {str(e)}'
        }


def main(session: Session, stage_name: str, target_table_name: str) -> str:
    """
    Main orchestration function for stage to table MERGE operation.
    
    Args:
        session: Snowflake session object
        stage_name: Name of the stage containing CSV files
        target_table_name: Name of the target table
        
    Returns:
        String with execution summary
    """
    execution_log = []
    staging_table_name = None
    pk_cols = []
    
    try:
        # Step 1: Get primary key columns from target table
        execution_log.append("=" * 60)
        execution_log.append("STEP 1: Detecting primary key columns from target table")
        result = get_primary_key_columns(session, target_table_name)
        execution_log.append(f"Status: {result['status']}")
        execution_log.append(f"Message: {result['message']}")
        
        pk_cols = result['pk_columns']
        
        # If no primary key found, try unique keys
        if not pk_cols or len(pk_cols) == 0:
            execution_log.append("Attempting to find unique key columns as fallback...")
            result = get_unique_key_columns(session, target_table_name)
            execution_log.append(f"Unique key status: {result['status']}")
            execution_log.append(f"Message: {result['message']}")
            pk_cols = result.get('unique_columns', [])
        
        # If still no keys found, use first column as default
        if not pk_cols or len(pk_cols) == 0:
            execution_log.append("No primary or unique keys found. Using first column as merge key...")
            describe_sql = f"DESCRIBE TABLE {target_table_name}"
            columns_result = session.sql(describe_sql).collect()
            if columns_result and len(columns_result) > 0:
                pk_cols = [columns_result[0]['name']]
                execution_log.append(f"Using column: {pk_cols[0]}")
            else:
                execution_log.append("ERROR: Could not determine any columns from target table")
                return '\n'.join(execution_log)
        
        execution_log.append(f"Merge will use columns: {', '.join(pk_cols)}")
        
        # Step 2: Generate staging table name
        execution_log.append("\n" + "=" * 60)
        execution_log.append("STEP 2: Generating staging table name")
        staging_table_name = generate_staging_table_name(target_table_name)
        execution_log.append(f"Generated staging table: {staging_table_name}")
        
        # Step 3: Create staging table
        execution_log.append("\n" + "=" * 60)
        execution_log.append("STEP 3: Creating staging table")
        result = create_staging_table(session, staging_table_name, target_table_name)
        execution_log.append(f"Status: {result['status']}")
        execution_log.append(f"Message: {result['message']}")
        if result['status'] == 'error':
            return '\n'.join(execution_log)
        
        # Step 4: Copy from stage to staging table
        execution_log.append("\n" + "=" * 60)
        execution_log.append("STEP 4: Copying data from stage to staging table")
        result = copy_from_stage_to_staging(session, stage_name, staging_table_name)
        execution_log.append(f"Status: {result['status']}")
        execution_log.append(f"Message: {result['message']}")
        execution_log.append(f"Rows loaded: {result['rows_loaded']}")
        if result['status'] == 'error':
            return '\n'.join(execution_log)
        
        rows_loaded = result['rows_loaded']
        
        # Step 5: Verify record count
        execution_log.append("\n" + "=" * 60)
        execution_log.append("STEP 5: Verifying record count")
        result = verify_record_count(session, staging_table_name, rows_loaded)
        execution_log.append(f"Status: {result['status']}")
        execution_log.append(f"Message: {result['message']}")
        execution_log.append(f"Actual count: {result['actual_count']}")
        if result['status'] == 'error':
            return '\n'.join(execution_log)
        
        # Step 6: Perform MERGE
        execution_log.append("\n" + "=" * 60)
        execution_log.append("STEP 6: Performing MERGE operation")
        result = perform_merge(session, staging_table_name, target_table_name, pk_cols)
        execution_log.append(f"Status: {result['status']}")
        execution_log.append(f"Message: {result['message']}")
        execution_log.append(f"Rows inserted: {result['rows_inserted']}")
        if result['status'] == 'error':
            return '\n'.join(execution_log)
        
        # Step 7: Cleanup staging table
        execution_log.append("\n" + "=" * 60)
        execution_log.append("STEP 7: Cleaning up staging table")
        result = cleanup_staging_table(session, staging_table_name)
        execution_log.append(f"Status: {result['status']}")
        execution_log.append(f"Message: {result['message']}")
        
        # Final summary
        execution_log.append("\n" + "=" * 60)
        execution_log.append("EXECUTION COMPLETED SUCCESSFULLY")
        execution_log.append("=" * 60)
        
        return '\n'.join(execution_log)
        
    except Exception as e:
        execution_log.append("\n" + "=" * 60)
        execution_log.append(f"CRITICAL ERROR: {str(e)}")
        execution_log.append("=" * 60)
        
        # Try to cleanup if staging table was created
        if staging_table_name:
            try:
                cleanup_staging_table(session, staging_table_name)
                execution_log.append(f"Staging table {staging_table_name} cleaned up after error")
            except:
                execution_log.append(f"Failed to cleanup staging table {staging_table_name}")
        
        return '\n'.join(execution_log)
$$;

-- Example usage:
-- CALL stage_to_table_merge('MY_STAGE', 'MY_TARGET_TABLE');
-- CALL stage_to_table_merge('@MY_STAGE', 'CUSTOMER_DATA');
-- CALL stage_to_table_merge('DOTNET_EXEC_STAGE', 'MY_TABLE');
