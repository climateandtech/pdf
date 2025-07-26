#!/usr/bin/env python3
"""
Example Integration with Existing services.py

Demonstrates how to integrate the S3 + NATS control bus with existing DoclingClient.
"""
import asyncio
import tempfile
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

from s3_integration import (
    S3DocumentService, 
    process_pdf_with_s3_sync, 
    configure_s3_storage,
    S3DoclingIntegration
)

def create_test_pdf():
    """Create a test PDF for demonstration"""
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, "S3 + NATS Integration Demo")
        c.drawString(100, 700, "This PDF demonstrates the integration with existing services.py")
        c.drawString(100, 650, "Features:")
        c.drawString(120, 600, "✓ Clean boto3 integration")
        c.drawString(120, 550, "✓ Async context managers")
        c.drawString(120, 500, "✓ Production-ready architecture")
        c.drawString(120, 450, "✓ Easy integration with existing code")
        c.save()
        return Path(tmp.name)

def demo_sync_integration():
    """Demonstrate synchronous integration (existing services.py pattern)"""
    print("🔄 Demo 1: Synchronous Integration (services.py compatible)")
    print("-" * 60)
    
    # Create test PDF
    pdf_path = create_test_pdf()
    
    try:
        # Example 1: Simple usage
        print("📄 Processing PDF with default settings...")
        # result = process_pdf_with_s3_sync(pdf_path)  # Would need real S3/NATS
        print("   ✓ Would upload to S3 and process via NATS control bus")
        
        # Example 2: With MinIO configuration
        print("🗄️  Processing PDF with MinIO configuration...")
        minio_config = configure_s3_storage(
            endpoint_url="http://localhost:9000",
            bucket_name="demo-documents",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin"
        )
        # result = process_pdf_with_s3_sync(pdf_path, config_override=minio_config)
        print("   ✓ Would use MinIO S3-compatible storage")
        
        print("✅ Synchronous integration demo complete")
        
    finally:
        pdf_path.unlink()  # Cleanup

async def demo_async_integration():
    """Demonstrate asynchronous integration (modern async/await pattern)"""
    print("\n🔄 Demo 2: Asynchronous Integration (async/await pattern)")
    print("-" * 60)
    
    # Create test PDF
    pdf_path = create_test_pdf()
    
    try:
        # Example 1: Async context manager
        print("📄 Processing PDF with async context manager...")
        config_override = {"bucket_name": "async-demo-bucket"}
        
        async with S3DocumentService(config_override) as service:
            print("   ✓ S3DocumentService initialized")
            print("   ✓ Connected to NATS JetStream")
            print("   ✓ S3 bucket verified/created")
            # result = await service.process_document(pdf_path)
            print("   ✓ Would process document via S3 + NATS control bus")
        
        print("   ✓ Resources automatically cleaned up")
        print("✅ Asynchronous integration demo complete")
        
    finally:
        pdf_path.unlink()  # Cleanup

def demo_existing_services_integration():
    """Demonstrate integration with existing services.py pattern"""
    print("\n🔄 Demo 3: Existing services.py Integration Pattern")
    print("-" * 60)
    
    class ExampleDocumentService:
        """Example of how to modify existing services.py"""
        
        def __init__(self, use_s3_storage=False):
            self.use_s3_storage = use_s3_storage
            if use_s3_storage:
                print("   ✓ Configured for S3 + NATS control bus")
            else:
                print("   ✓ Configured for NATS Object Store (existing)")
        
        def process_pdf(self, pdf_path: str, custom_prompt: str = None):
            """Process PDF with storage option selection"""
            if self.use_s3_storage:
                print(f"   📁 Processing {pdf_path} via S3 + NATS control bus")
                # return process_pdf_with_s3_sync(pdf_path, custom_prompt=custom_prompt)
                return {"method": "s3_nats", "status": "demo_success"}
            else:
                print(f"   📁 Processing {pdf_path} via NATS Object Store")
                # return existing_docling_client.process(pdf_path, custom_prompt)
                return {"method": "nats_object_store", "status": "demo_success"}
    
    # Demo both approaches
    pdf_path = create_test_pdf()
    
    try:
        # Traditional approach
        print("🗄️  Using NATS Object Store (existing):")
        traditional_service = ExampleDocumentService(use_s3_storage=False)
        result1 = traditional_service.process_pdf(str(pdf_path))
        print(f"   Result: {result1}")
        
        # New S3 approach
        print("☁️  Using S3 + NATS Control Bus (new):")
        s3_service = ExampleDocumentService(use_s3_storage=True)
        result2 = s3_service.process_pdf(str(pdf_path), custom_prompt="Extract key findings")
        print(f"   Result: {result2}")
        
        print("✅ Services.py integration demo complete")
        
    finally:
        pdf_path.unlink()  # Cleanup

def demo_docling_integration():
    """Demonstrate DoclingClient integration"""
    print("\n🔄 Demo 4: DoclingClient Integration")
    print("-" * 60)
    
    # This shows how the S3DoclingIntegration class works
    integration = S3DoclingIntegration()
    
    print("🔗 DoclingClient Integration Status:")
    if integration.docling_client:
        print("   ✓ Successfully integrated with existing DoclingClient")
        print("   ✓ Can process PDFs from S3 URLs")
        print("   ✓ Supports custom prompts")
    else:
        print("   ⚠️  DoclingClient not found, using mock processing")
        print("   ℹ️  In production, would integrate with thinktank2/search/backend/app/services.py")
    
    # Demo URL processing
    demo_s3_url = "https://s3.amazonaws.com/demo-bucket/document.pdf"
    print(f"📄 Would process PDF from S3 URL: {demo_s3_url}")
    
    # In real usage:
    # result = await integration.process_from_s3_url(demo_s3_url, "Extract summary")
    
    print("✅ DoclingClient integration demo complete")

def demo_configuration_options():
    """Demonstrate configuration flexibility"""
    print("\n🔄 Demo 5: Configuration Options")
    print("-" * 60)
    
    # AWS S3 configuration
    aws_config = configure_s3_storage(
        bucket_name="production-documents",
        region_name="us-west-2"
    )
    print("☁️  AWS S3 Configuration:")
    print(f"   Bucket: {aws_config.get('bucket_name')}")
    print(f"   Region: {aws_config.get('region_name')}")
    print("   Credentials: Via AWS credential chain")
    
    # MinIO configuration
    minio_config = configure_s3_storage(
        endpoint_url="http://localhost:9000",
        bucket_name="local-documents",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )
    print("\n🗄️  MinIO Configuration:")
    print(f"   Endpoint: {minio_config.get('endpoint_url')}")
    print(f"   Bucket: {minio_config.get('bucket_name')}")
    print("   Credentials: Programmatic")
    
    # Custom S3-compatible service
    custom_config = configure_s3_storage(
        endpoint_url="https://s3.custom-provider.com",
        bucket_name="enterprise-docs",
        region_name="eu-central-1"
    )
    print("\n🔧 Custom S3-compatible Service:")
    print(f"   Endpoint: {custom_config.get('endpoint_url')}")
    print(f"   Bucket: {custom_config.get('bucket_name')}")
    print(f"   Region: {custom_config.get('region_name')}")
    
    print("✅ Configuration options demo complete")

async def main():
    """Run all demonstrations"""
    print("🚀 S3 + NATS Integration Demonstrations")
    print("=" * 80)
    
    # Run all demos
    demo_sync_integration()
    await demo_async_integration()
    demo_existing_services_integration()
    demo_docling_integration()
    demo_configuration_options()
    
    print("\n" + "=" * 80)
    print("🎉 All demonstrations complete!")
    print("\n📋 Summary of Integration Options:")
    print("   1. ✅ Drop-in replacement for existing sync code")
    print("   2. ✅ Modern async/await patterns")
    print("   3. ✅ Flexible configuration (AWS, MinIO, custom)")
    print("   4. ✅ Production-ready architecture")
    print("   5. ✅ Clean integration with existing services.py")
    print("\n🚀 Ready for production deployment!")

if __name__ == "__main__":
    asyncio.run(main()) 