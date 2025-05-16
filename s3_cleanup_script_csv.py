def export_to_csv(objects_to_delete, csv_filename=None):
    """
    Export the list of objects to a CSV file in the current working directory
    
    Args:
        objects_to_delete (list): List of objects to export
        csv_filename (str, optional): Name of CSV file. If None, a default name with timestamp will be created
        
    Returns:
        str: Path to the created CSV file
    """
    if not csv_filename:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"s3_cleanup_{timestamp}.csv"
    
    # Ensure the path is relative to current working directory
    csv_path = os.path.join(os.getcwd(), csv_filename)
    
    try:
        with open(csv_path, 'w', newline='') as csvfile:
            # Define CSV columns
            fieldnames = ['Object_Key', 'Last_Modified', 'Size_Bytes', 'Size_KB', 'Size_MB']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write data rows
            for obj in objects_to_delete:
                writer.writerow({
                    'Object_Key': obj['Key'],
                    'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'Size_Bytes': obj['Size'],
                    'Size_KB': round(obj['Size'] / 1024, 2),
                    'Size_MB': round(obj['Size'] / (1024 * 1024), 4)
                })
        
        print(f"Exported list of objects to: {csv_path}")
        return csv_path
        
    except Exception as e:
        print(f"Error exporting to CSV: {str(e)}")
        return None
