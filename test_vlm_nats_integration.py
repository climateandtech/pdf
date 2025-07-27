#!/usr/bin/env python3
"""
VLM NATS Integration Test
Tests that simple JSON VLM options work over the distributed system
"""

import asyncio
import json
from services import DocumentService
from pathlib import Path

async def test_vlm_nats_integration():
    """Test VLM processing through the distributed NATS system"""
    
    print("🚀 VLM NATS Integration Test")
    print("=" * 50)
    
    # Test 1: Simple JSON VLM options (what we send over NATS)
    vlm_options_granite = {
        'vlm_model': 'granite',
        'do_picture_description': True,
        'images_scale': 2.0,
        'custom_prompt': 'Describe this image in detail, focusing on the main elements.'
    }
    
    vlm_options_smol = {
        'vlm_model': 'smoldocling',  # Should map to SmolVLM
        'do_picture_description': True,
        'images_scale': 1.5,
        'custom_prompt': 'What do you see in this image?'
    }
    
    # Connect to services
    print("🔌 Connecting to DocumentService...")
    doc_service = DocumentService()
    await doc_service.setup()
    print("✅ Connected to NATS and S3")
    
    # Load test PDF
    test_pdf_path = Path("../testpdf.pdf")
    if not test_pdf_path.exists():
        print("❌ testpdf.pdf not found!")
        return
    
    with open(test_pdf_path, 'rb') as f:
        pdf_content = f.read()
    
    print(f"📄 Loaded testpdf.pdf: {len(pdf_content)} bytes")
    
    # Test both VLM models
    test_cases = [
        ("Granite Vision", vlm_options_granite),
        ("SmolVLM", vlm_options_smol)
    ]
    
    for model_name, vlm_options in test_cases:
        print(f"\n🎯 Testing {model_name}")
        print("-" * 30)
        
        try:
            # Upload PDF
            s3_key = await doc_service.store_document(pdf_content, f'vlm_test_{model_name.lower().replace(" ", "_")}')
            print(f"✅ PDF uploaded: {s3_key}")
            
            # Send VLM processing request
            print(f"📤 Sending VLM options: {vlm_options}")
            print("📡 Processing via NATS worker...")
            
            result = await doc_service.process_document(s3_key, docling_options=vlm_options)
            
            if result['status'] == 'success':
                print("✅ Worker processing successful!")
                
                # Analyze results for VLM descriptions
                structured_data = result.get('result', {}).get('structured_data', {})
                pictures = structured_data.get('pictures', [])
                
                print(f"📸 Pictures found: {len(pictures)}")
                
                vlm_descriptions = 0
                for i, pic in enumerate(pictures, 1):
                    annotations = pic.get('annotations', [])
                    print(f"   Picture {i}: {len(annotations)} annotations")
                    
                    # Look for VLM descriptions
                    for ann in annotations:
                        if isinstance(ann, dict) and 'text' in ann:
                            description = ann['text']
                            if description and len(description) > 20:  # Meaningful description
                                print(f"     🤖 VLM: {description[:100]}...")
                                vlm_descriptions += 1
                        elif hasattr(ann, 'text') and ann.text:
                            description = ann.text
                            print(f"     🤖 VLM: {description[:100]}...")
                            vlm_descriptions += 1
                
                print(f"🎉 {model_name}: {vlm_descriptions} VLM descriptions generated")
                
                if vlm_descriptions == 0:
                    print("❌ No VLM descriptions found - checking raw annotations...")
                    for i, pic in enumerate(pictures[:1], 1):  # Check first picture only
                        print(f"     Picture {i} raw annotations: {pic.get('annotations', [])}")
                
            else:
                print(f"❌ Worker processing failed: {result}")
                
        except Exception as e:
            print(f"❌ Test failed for {model_name}: {e}")
    
    print(f"\n📊 Integration test complete!")

async def test_worker_conversion_logic():
    """Test the worker's VLM option conversion logic locally"""
    
    print("\n🔧 Testing Worker VLM Conversion Logic")
    print("=" * 50)
    
    # Import worker for testing
    from docling_worker import DoclingWorker
    
    worker = DoclingWorker()
    
    # Test cases for option conversion
    test_cases = [
        {
            "name": "Granite VLM",
            "options": {'vlm_model': 'granite', 'do_picture_description': True, 'images_scale': 2.0}
        },
        {
            "name": "SmolVLM", 
            "options": {'vlm_model': 'smoldocling', 'do_picture_description': True}
        },
        {
            "name": "Custom Prompt",
            "options": {'vlm_model': 'granite', 'do_picture_description': True, 'custom_prompt': 'Custom test prompt'}
        },
        {
            "name": "No VLM",
            "options": {'some_other_option': 'value'}
        }
    ]
    
    for test_case in test_cases:
        print(f"\n🧪 Testing: {test_case['name']}")
        options = test_case['options']
        
        # Test detection
        is_simple = worker._is_simple_vlm_options(options)
        print(f"   Simple VLM options detected: {is_simple}")
        
        if is_simple:
            # Test conversion
            try:
                docling_config = worker._convert_simple_vlm_options(options)
                print(f"   ✅ Conversion successful: {list(docling_config.keys())}")
                
                # Test that we can create a DocumentConverter
                converter = worker._create_document_converter(options)
                print(f"   ✅ DocumentConverter created successfully")
                
            except Exception as e:
                print(f"   ❌ Conversion failed: {e}")
        else:
            print(f"   ➡️  Not simple VLM options, would pass through as-is")

if __name__ == "__main__":
    async def main():
        await test_worker_conversion_logic()
        await test_vlm_nats_integration()
    
    asyncio.run(main()) 