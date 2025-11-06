"""
Local test script for STAC conversion.
Use this to test the conversion process locally before running batch jobs.
"""

import os
import json
from azure.storage.blob import ContainerClient, BlobServiceClient
from azure.identity import DefaultAzureCredential
from utils.azure_storage_utils import get_blob_service_client, ensure_container_exists

# Import conversion functions
from stac_conversion import process_cog_to_stac, save_stac_item_to_blob


def test_single_cog_conversion(upload_to_blob=False):
    """
    Test converting a single COG file to STAC item locally.
    
    Parameters:
        upload_to_blob (bool): Whether to upload the STAC item to Azure Blob Storage
    """
    print("=" * 60)
    print("STAC Conversion Local Test")
    print("=" * 60)
    
    # Configuration
    blob_domain = os.environ.get("STORAGE_ACCOUNT_URL", "https://mpcpstorageaccount.blob.core.windows.net")
    container_name = "processed-cogs"
    year = "1981"  # Test with 1981 data
    
    print(f"Storage Account: {blob_domain}")
    print(f"Container: {container_name}")
    print(f"Testing with year: {year}")
    print(f"Upload to Blob: {'Yes' if upload_to_blob else 'No (local only)'}")
    print("-" * 60)
    
    try:
        # Setup Azure connection
        credential = DefaultAzureCredential()
        container_client = ContainerClient(
            account_url=blob_domain,
            container_name=container_name,
            credential=credential
        )
        
        # Get first COG file from the year directory
        directory_path = f"{year}/"
        print(f"Looking for COG files in: {directory_path}")
        
        blobs = list(container_client.list_blobs(name_starts_with=directory_path))
        
        if not blobs:
            print(f"❌ No COG files found in {directory_path}")
            return
        
        # Test with first blob
        first_blob = blobs[0]
        print(f"Found {len(blobs)} COG files")
        print(f"Testing with: {first_blob.name}")
        print("-" * 60)
        
        # Get blob client
        blob_client = container_client.get_blob_client(first_blob.name)
        filename = os.path.basename(first_blob.name)
        
        # Process COG to STAC
        print("Converting COG to STAC item...")
        stac_item_dict = process_cog_to_stac(
            blob_client=blob_client,
            filename=filename,
            blob_domain=blob_domain,
            container_name=container_name,
            year=year
        )
        
        print("✅ Conversion successful!")
        print("-" * 60)
        
        # Display STAC item properties
        print("STAC Item Properties:")
        print(f"  ID: {stac_item_dict['id']}")
        print(f"  Type: {stac_item_dict['type']}")
        print(f"  STAC Version: {stac_item_dict['stac_version']}")
        
        properties = stac_item_dict.get('properties', {})
        print(f"  Product Type: {properties.get('product_type')}")
        print(f"  Version: {properties.get('version')}")
        print(f"  Start DateTime: {properties.get('start_datetime')}")
        print(f"  File Size: {properties.get('file:size')} bytes")
        
        # Save to local file for inspection
        output_file = f"test_stac_item_{year}.json"
        with open(output_file, 'w') as f:
            json.dump(stac_item_dict, f, indent=2)
        
        print("-" * 60)
        print(f"📄 STAC item saved locally to: {output_file}")
        
        # Upload to blob if requested
        if upload_to_blob:
            print("-" * 60)
            print("Uploading STAC item to Azure Blob Storage...")
            
            # Setup blob service client for upload
            blob_service_client = BlobServiceClient(
                account_url=blob_domain,
                credential=credential
            )
            
            # Check if stac-items container exists, create if not
            stac_container_name = "stac-items"
            try:
                stac_container_client = blob_service_client.get_container_client(stac_container_name)
                stac_container_client.get_container_properties()
                print(f"✅ Container '{stac_container_name}' exists")
            except:
                stac_container_client.create_container()
                print(f"✅ Created container '{stac_container_name}'")
            
            # Prepare blob path
            stac_filename = filename.replace('.tif', '.json')
            stac_blob_path = f"{year}/{stac_filename}"
            
            # Upload STAC item
            save_stac_item_to_blob(
                stac_item_dict=stac_item_dict,
                blob_service_client=blob_service_client,
                container_name=stac_container_name,
                blob_path=stac_blob_path
            )
            
            print(f"✅ STAC item uploaded to: {stac_container_name}/{stac_blob_path}")
            print(f"📍 Full URL: {blob_domain}/{stac_container_name}/{stac_blob_path}")
        
        print("-" * 60)
        print("Test completed successfully!")
        
        return stac_item_dict
        
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_batch_simulation(max_files: int = 5, upload_to_blob: bool = False):
    """
    Simulate batch processing with a small number of files.
    
    Parameters:
        max_files (int): Maximum number of files to process
        upload_to_blob (bool): Whether to upload STAC items to Azure Blob Storage
    """
    print("\n" + "=" * 60)
    print(f"Batch Processing Simulation (max {max_files} files)")
    print("=" * 60)
    
    blob_domain = os.environ.get("STORAGE_ACCOUNT_URL", "https://mpcpstorageaccount.blob.core.windows.net")
    container_name = "processed-cogs"
    year = "1981"
    
    print(f"Upload to Blob: {'Yes' if upload_to_blob else 'No (local only)'}")
    print("-" * 60)
    
    try:
        # Setup Azure connection
        credential = DefaultAzureCredential()
        container_client = ContainerClient(
            account_url=blob_domain,
            container_name=container_name,
            credential=credential
        )
        
        # Setup blob service client if uploading
        blob_service_client = None
        if upload_to_blob:
            blob_service_client = get_blob_service_client()
            
            # Check/create stac-items container
            stac_container_name = "stac-items"
            ensure_container_exists(blob_service_client, stac_container_name)
        
        # Get COG files
        directory_path = f"{year}/"
        blobs = list(container_client.list_blobs(name_starts_with=directory_path))[:max_files]
        
        if not blobs:
            print(f"No COG files found in {directory_path}")
            return
        
        print(f"Processing {len(blobs)} files...")
        print("-" * 60)
        
        successful = 0
        failed = 0
        uploaded = 0
        
        for i, blob in enumerate(blobs, 1):
            try:
                print(f"[{i}/{len(blobs)}] Processing: {blob.name}")
                
                blob_client = container_client.get_blob_client(blob.name)
                filename = os.path.basename(blob.name)
                
                stac_item_dict = process_cog_to_stac(
                    blob_client=blob_client,
                    filename=filename,
                    blob_domain=blob_domain,
                    container_name=container_name,
                    year=year
                )
                
                print(f"    ✅ Success - ID: {stac_item_dict['id']}")
                successful += 1
                
                # Upload if requested
                if upload_to_blob and blob_service_client:
                    stac_filename = filename.replace('.tif', '.json')
                    stac_blob_path = f"{year}/{stac_filename}"
                    
                    save_stac_item_to_blob(
                        stac_item_dict=stac_item_dict,
                        blob_service_client=blob_service_client,
                        container_name="stac-items",
                        blob_path=stac_blob_path
                    )
                    print(f"    📤 Uploaded to: stac-items/{stac_blob_path}")
                    uploaded += 1
                
            except Exception as e:
                print(f"    ❌ Failed: {e}")
                failed += 1
        
        print("-" * 60)
        print(f"Simulation Complete:")
        print(f"  ✅ Successful: {successful}")
        print(f"  ❌ Failed: {failed}")
        if upload_to_blob:
            print(f"  📤 Uploaded: {uploaded}")
        print(f"  Success Rate: {(successful/len(blobs)*100):.1f}%")
        
    except Exception as e:
        print(f"❌ Simulation error: {e}")
        import traceback
        traceback.print_exc()


def test_all_1981_files(upload_to_blob: bool = False):
    """
    Test converting all 1981 COG files to STAC items.
    
    Parameters:
        upload_to_blob (bool): Whether to upload STAC items to Azure Blob Storage
    """
    print("\n" + "=" * 60)
    print("Testing All 1981 Files")
    print("=" * 60)
    
    blob_domain = os.environ.get("STORAGE_ACCOUNT_URL", "https://mpcpstorageaccount.blob.core.windows.net")
    container_name = "processed-cogs"
    year = "1981"
    
    print(f"Year: {year}")
    print(f"Upload to Blob: {'Yes' if upload_to_blob else 'No (local only)'}")
    print("-" * 60)
    
    try:
        # Setup Azure connection
        credential = DefaultAzureCredential()
        container_client = ContainerClient(
            account_url=blob_domain,
            container_name=container_name,
            credential=credential
        )
        
        # Setup blob service client if uploading
        blob_service_client = None
        if upload_to_blob:
            blob_service_client = get_blob_service_client()
            
            # Check/create stac-items container
            stac_container_name = "stac-items"
            try:
                stac_container_client = blob_service_client.get_container_client(stac_container_name)
                stac_container_client.get_container_properties()
                print(f"✅ Container '{stac_container_name}' exists")
            except:
                stac_container_client.create_container()
                print(f"✅ Created container '{stac_container_name}'")
        
        # Get ALL COG files from 1981
        directory_path = f"{year}/"
        print(f"Scanning for all files in: {directory_path}")
        blobs = list(container_client.list_blobs(name_starts_with=directory_path))
        
        if not blobs:
            print(f"❌ No COG files found in {directory_path}")
            return
        
        print(f"Found {len(blobs)} COG files for {year}")
        
        # Ask for confirmation if many files
        if len(blobs) > 50 and upload_to_blob:
            response = input(f"\n⚠️  This will upload {len(blobs)} STAC items to Azure. Continue? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled.")
                return
        
        print(f"\nProcessing {len(blobs)} files...")
        print("-" * 60)
        
        successful = 0
        failed = 0
        uploaded = 0
        failed_files = []
        
        # Process with progress updates
        for i, blob in enumerate(blobs, 1):
            try:
                # Show progress every 10 files or at start
                if i == 1 or i % 10 == 0 or i == len(blobs):
                    print(f"\n📊 Progress: {i}/{len(blobs)} ({(i/len(blobs)*100):.1f}%)")
                
                blob_client = container_client.get_blob_client(blob.name)
                filename = os.path.basename(blob.name)
                
                # Convert to STAC
                stac_item_dict = process_cog_to_stac(
                    blob_client=blob_client,
                    filename=filename,
                    blob_domain=blob_domain,
                    container_name=container_name,
                    year=year
                )
                
                successful += 1
                
                # Upload if requested
                if upload_to_blob and blob_service_client:
                    stac_filename = filename.replace('.tif', '.json')
                    stac_blob_path = f"{year}/{stac_filename}"
                    
                    save_stac_item_to_blob(
                        stac_item_dict=stac_item_dict,
                        blob_service_client=blob_service_client,
                        container_name="stac-items",
                        blob_path=stac_blob_path
                    )
                    uploaded += 1
                    
                    # Show detailed progress for first few and then summary
                    if i <= 3:
                        print(f"  ✅ {filename} -> {stac_blob_path}")
                
            except Exception as e:
                failed += 1
                failed_files.append({"file": blob.name, "error": str(e)})
                print(f"  ❌ Failed: {filename} - {str(e)[:50]}...")
        
        # Final summary
        print("\n" + "=" * 60)
        print(f"Processing Complete for Year {year}:")
        print("=" * 60)
        print(f"  📁 Total Files: {len(blobs)}")
        print(f"  ✅ Successful: {successful}")
        print(f"  ❌ Failed: {failed}")
        if upload_to_blob:
            print(f"  📤 Uploaded: {uploaded}")
        print(f"  📈 Success Rate: {(successful/len(blobs)*100):.1f}%")
        
        # Show failed files if any
        if failed_files:
            print("\n❌ Failed Files:")
            for fail in failed_files[:5]:  # Show first 5
                print(f"  - {fail['file']}: {fail['error'][:50]}...")
            if len(failed_files) > 5:
                print(f"  ... and {len(failed_files) - 5} more")
        
        # Save summary to file
        summary_file = f"stac_conversion_summary_{year}.json"
        summary = {
            "year": year,
            "total_files": len(blobs),
            "successful": successful,
            "failed": failed,
            "uploaded": uploaded if upload_to_blob else 0,
            "success_rate": (successful/len(blobs)*100),
            "failed_files": failed_files
        }
        
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n📄 Summary saved to: {summary_file}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


def validate_environment():
    """
    Validate that all required environment variables are set.
    """
    print("=" * 60)
    print("Environment Validation")
    print("=" * 60)
    
    required_vars = [
        "STORAGE_ACCOUNT_URL",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "BATCH_ACCOUNT_URL",
        "BATCH_STORAGE_ACCOUNT_KEY"
    ]
    
    optional_vars = [
        "AZ_BATCH_TASK_ID",
        "AZ_BATCH_TASK_WORKING_DIR"
    ]
    
    all_valid = True
    
    print("Required Environment Variables:")
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            # Mask sensitive values
            if "SECRET" in var or "KEY" in var:
                display_value = value[:4] + "****" + value[-4:] if len(value) > 8 else "****"
            else:
                display_value = value
            print(f"  ✅ {var}: {display_value}")
        else:
            print(f"  ❌ {var}: NOT SET")
            all_valid = False
    
    print("\nOptional Environment Variables (for batch execution):")
    for var in optional_vars:
        value = os.environ.get(var)
        if value:
            print(f"  ✅ {var}: {value}")
        else:
            print(f"  ⚠️  {var}: Not set (normal for local testing)")
    
    print("-" * 60)
    if all_valid:
        print("✅ All required environment variables are set")
    else:
        print("❌ Some required environment variables are missing")
        print("Please set them before running the batch job")
    
    return all_valid


def main():
    """
    Main test function with enhanced menu options.
    """
    import sys
    
    print("\nSTAC Conversion Test Suite")
    print("=" * 60)
    
    # Validate environment first
    env_valid = validate_environment()
    if not env_valid:
        print("\n⚠️  Warning: Environment validation failed")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            print("Exiting...")
            return
    
    while True:
        print("\n" + "=" * 60)
        print("Select test option:")
        print("=" * 60)
        print("1. Test single COG conversion (local only)")
        print("2. Test single COG conversion + upload to Azure")
        print("3. Test batch simulation - 5 files (local only)")
        print("4. Test batch simulation - 5 files + upload to Azure")
        print("5. Convert ALL 1981 files (local only)")
        print("6. Convert ALL 1981 files + upload to Azure")
        print("7. Custom batch test (specify number of files)")
        print("8. Validate environment")
        print("9. Exit")
        print("-" * 60)
        
        choice = input("Enter choice (1-9): ")
        
        if choice == "1":
            test_single_cog_conversion(upload_to_blob=False)
        
        elif choice == "2":
            print("\n⚠️  This will upload the STAC item to Azure Blob Storage")
            confirm = input("Continue? (y/n): ")
            if confirm.lower() == 'y':
                test_single_cog_conversion(upload_to_blob=True)
        
        elif choice == "3":
            test_batch_simulation(max_files=5, upload_to_blob=False)
        
        elif choice == "4":
            print("\n⚠️  This will upload 5 STAC items to Azure Blob Storage")
            confirm = input("Continue? (y/n): ")
            if confirm.lower() == 'y':
                test_batch_simulation(max_files=5, upload_to_blob=True)
        
        elif choice == "5":
            print("\n⚠️  This will process ALL 1981 files locally (may take time)")
            confirm = input("Continue? (y/n): ")
            if confirm.lower() == 'y':
                test_all_1981_files(upload_to_blob=False)
        
        elif choice == "6":
            print("\n⚠️  This will process and upload ALL 1981 files to Azure")
            print("This may take significant time and incur Azure costs")
            confirm = input("Are you sure you want to continue? (y/n): ")
            if confirm.lower() == 'y':
                test_all_1981_files(upload_to_blob=True)
        
        elif choice == "7":
            try:
                num_files = input("Enter number of files to test (1-365): ")
                num_files = int(num_files)
                if 1 <= num_files <= 365:
                    upload_choice = input("Upload to Azure? (y/n): ").lower() == 'y'
                    test_batch_simulation(max_files=num_files, upload_to_blob=upload_choice)
                else:
                    print("Invalid number. Please enter between 1 and 365.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        
        elif choice == "8":
            validate_environment()
        
        elif choice == "9":
            print("Exiting...")
            break
        
        else:
            print("Invalid choice. Please try again.")
        
        # Ask if user wants to continue
        if choice in ["1", "2", "3", "4", "5", "6", "7"]:
            another = input("\nRun another test? (y/n): ")
            if another.lower() != 'y':
                print("Exiting...")
                break


if __name__ == "__main__":
    main()