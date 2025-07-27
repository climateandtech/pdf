#!/usr/bin/env python3
"""
Docling Worker Service

This service:
1. Listens for NATS messages with S3 file locations
2. Downloads files from S3 
3. Processes them with docling
4. Sends results back via NATS

Replace HTTP API with NATS messaging.
"""
import asyncio
import json
import logging
from pathlib import Path
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, granite_picture_description

from s3_client import S3DocumentClient
from s3_config import S3Config
from config import NatsConfig

logger = logging.getLogger(__name__)

class DoclingWorker:
    """Docling processing worker that communicates via NATS"""
    
    def __init__(self):
        # S3 + NATS client
        self.s3_config = S3Config()
        self.nats_config = NatsConfig()
        self.client = S3DocumentClient(self.s3_config, self.nats_config)
        
        # Standard document converter - VLM will be configured per-request
        print("🤖 Docling Worker: Dynamic VLM configuration enabled")
        print("📋 VLM options will be specified by the publisher for each request")
        self.doc_converter = DocumentConverter()
        
    async def setup(self):
        """Initialize connections"""
        await self.client.setup()
        print(f"✅ Docling Worker connected to NATS: {self.nats_config.url}")
        print(f"✅ Docling Worker connected to S3: {self.s3_config.bucket_name}")
        
    def _create_document_converter(self, docling_options=None):
        """Create a DocumentConverter with configuration options
        
        Args:
            docling_options (dict): Can be either:
                - Simple JSON VLM options: {"vlm_model": "granite", "do_picture_description": true}
                - Complete Docling objects: {"format_options": {...}}
                
        Returns:
            DocumentConverter: Configured converter instance
        """
        if not docling_options:
            print("📋 Using standard DocumentConverter (no custom options)")
            return DocumentConverter()
        
        try:
            print(f"🎛️  Configuring DocumentConverter with options: {list(docling_options.keys())}")
            
            # Check if we have simple JSON VLM options (sent over NATS)
            if self._is_simple_vlm_options(docling_options):
                print("🔄 Converting simple VLM options to Docling objects...")
                docling_config = self._convert_simple_vlm_options(docling_options)
            else:
                print("📋 Using provided Docling configuration objects...")
                docling_config = docling_options
            
            # Create DocumentConverter with converted options
            converter = DocumentConverter(**docling_config)
            
            print("✅ DocumentConverter configured successfully")
            return converter
            
        except Exception as e:
            print(f"⚠️  DocumentConverter configuration failed: {e}")
            print("🔄 Falling back to standard converter")
            return DocumentConverter()
    
    def _is_simple_vlm_options(self, options):
        """Check if options are simple JSON VLM options (vs complex Docling objects)"""
        if not isinstance(options, dict):
            return False
        
        # Simple VLM options have keys like: vlm_model, do_picture_description, etc.
        simple_keys = {'vlm_model', 'do_picture_description', 'images_scale', 'custom_prompt'}
        complex_keys = {'format_options', 'accelerator_options'}
        
        has_simple = any(key in options for key in simple_keys)
        has_complex = any(key in options for key in complex_keys)
        
        return has_simple and not has_complex
    
    def _convert_simple_vlm_options(self, simple_options):
        """Convert simple JSON VLM options to proper Docling objects"""
        from docling.document_converter import PdfFormatOption
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import (
            PdfPipelineOptions, 
            granite_picture_description, 
            smolvlm_picture_description
        )
        
        # Start with basic pipeline options
        pipeline_options = PdfPipelineOptions()
        
        # Configure VLM if requested
        if simple_options.get('do_picture_description', False):
            pipeline_options.do_picture_description = True
            pipeline_options.generate_picture_images = True
            
            # Set image scale
            pipeline_options.images_scale = simple_options.get('images_scale', 2.0)
            
            # Choose VLM model
            vlm_model = simple_options.get('vlm_model', 'granite').lower()
            if vlm_model == 'granite':
                pipeline_options.picture_description_options = granite_picture_description
                print("🤖 Using Granite Vision model for VLM")
            elif vlm_model == 'smoldocling' or vlm_model == 'smolvlm':
                pipeline_options.picture_description_options = smolvlm_picture_description
                print("🤖 Using SmolVLM model for VLM")
            else:
                print(f"⚠️  Unknown VLM model '{vlm_model}', defaulting to Granite")
                pipeline_options.picture_description_options = granite_picture_description
            
            # Custom prompt if provided
            custom_prompt = simple_options.get('custom_prompt')
            if custom_prompt and hasattr(pipeline_options.picture_description_options, 'prompt'):
                pipeline_options.picture_description_options.prompt = custom_prompt
                print(f"📝 Using custom VLM prompt: {custom_prompt[:50]}...")
        
        # Create format options
        format_options = {
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
        
        return {"format_options": format_options}

    async def process_document_request(self, message):
        """Process a document processing request from NATS"""
        try:
            # Parse the request
            request = json.loads(message.data.decode())
            request_id = request.get("request_id")
            s3_key = request.get("s3_key")
            docling_options = request.get("docling_options")  # Generic Docling configuration from publisher
            
            print(f"📨 Docling Worker: Received request {request_id} for {s3_key}")
            if docling_options:
                print(f"🎛️  Docling Options: {list(docling_options.keys())}")
            
            # Download file from S3
            print(f"⬇️  Docling Worker: Downloading {s3_key} from S3...")
            file_content = await self.client.download_result(s3_key)
            
            # Save to temporary file for docling
            temp_file = Path(f"/tmp/{request_id}.pdf")
            with open(temp_file, 'wb') as f:
                f.write(file_content)
                
            print(f"🔬 Docling Worker: Processing PDF with docling...")
            
            # **DYNAMIC DOCLING CONFIGURATION**
            doc_converter = self._create_document_converter(docling_options)
            
            # **REAL DOCLING PROCESSING**
            result = doc_converter.convert(str(temp_file))
            
            # Extract content in different formats
            document = result.document
            
            # Get markdown
            markdown_content = document.export_to_markdown()
            
            # Get structured data (if available)
            structured_data = None
            try:
                structured_data = document.export_to_dict()
                
                # **DETAILED PICTURE ANALYSIS**
                # Let's examine what VLM/annotation features are already available
                if isinstance(structured_data, dict):
                    pictures = structured_data.get("pictures", [])
                    if pictures:
                        print(f"📸 Docling Worker: Found {len(pictures)} pictures in document")
                        
                        # **VLM PICTURE DESCRIPTION ANALYSIS**
                        vlm_descriptions_found = 0
                        
                        for i, picture in enumerate(pictures):
                            print(f"🖼️  Picture {i+1} VLM analysis:")
                            print(f"   📍 Reference: {picture.get('self_ref', 'N/A')}")
                            
                            # Check for VLM annotations (following official Docling pattern)
                            annotations = picture.get("annotations", [])
                            if annotations and len(annotations) > 0:
                                vlm_descriptions_found += 1
                                print(f"   🎯 VLM Annotations found: {len(annotations)} annotation(s)")
                                
                                for j, annotation in enumerate(annotations):
                                    if isinstance(annotation, dict):
                                        # Check for text field in annotation
                                        if 'text' in annotation:
                                            vlm_text = annotation['text']
                                            provenance = annotation.get('provenance', 'VLM')
                                            preview = vlm_text[:150] + "..." if len(vlm_text) > 150 else vlm_text
                                            print(f"   🤖 {provenance}: {preview}")
                                        else:
                                            print(f"   🔍 Annotation {j+1}: {str(annotation)[:100]}...")
                                    else:
                                        preview = str(annotation)[:150] + "..." if len(str(annotation)) > 150 else str(annotation)
                                        print(f"   🤖 VLM Description: {preview}")
                            else:
                                print(f"   📝 No VLM annotations found")
                            
                            # Also check for any captions or additional description fields
                            if 'captions' in picture and picture['captions']:
                                print(f"   📋 Captions: {len(picture['captions'])} found")
                        
                        print(f"🤖 Docling Worker: VLM analysis complete - {vlm_descriptions_found}/{len(pictures)} descriptions generated")
                        
                        if vlm_descriptions_found > 0:
                            print(f"🎉 VLM SUCCESS: Generated descriptions for {vlm_descriptions_found} pictures!")
                        else:
                            print(f"⚠️  VLM: No descriptions generated - model may be loading or configuration issue")
                    else:
                        print(f"📷 Docling Worker: No pictures detected in document")
                        
            except Exception as e:
                print(f"⚠️  Docling Worker: Error extracting structured data: {e}")
                structured_data = None
                
            # Create response
            response = {
                "request_id": request_id,
                "status": "success",
                "result": {
                    "text": markdown_content,
                    "markdown": markdown_content,
                    "structured_data": structured_data,
                    "metadata": {
                        "pages": len(document.pages) if hasattr(document, 'pages') else 1,
                        "format": "pdf",
                        "processed_by": "docling_worker"
                    }
                }
            }
            
            print(f"✅ Docling Worker: Processing complete! Extracted {len(markdown_content)} characters")
            
            # Ensure results stream exists
            results_stream = f"{self.nats_config.stream_name}_results"
            try:
                await self.client.js.stream_info(results_stream)
            except Exception as e:
                if "not found" in str(e):
                    print(f"🔧 Creating results stream: {results_stream}")
                    await self.client.js.add_stream(
                        name=results_stream,
                        subjects=[f"{self.nats_config.subject_prefix}.result.*"],
                        storage="memory",
                        retention="limits",
                        max_msgs=1000,
                        max_bytes=100 * 1024 * 1024,  # 100MB
                        max_age=3600  # Keep results for 1 hour
                    )
            
            # Send response back via NATS
            await self.client.js.publish(
                f"{self.nats_config.subject_prefix}.result.{request_id}",
                json.dumps(response).encode()
            )
            
            print(f"📤 Docling Worker: Sent response for {request_id}")
            
            # Acknowledge the message
            await message.ack()
            
            # Cleanup
            temp_file.unlink()
            
        except Exception as e:
            print(f"❌ Docling Worker: Error processing request: {e}")
            
            # Send error response
            error_response = {
                "request_id": request.get("request_id", "unknown"),
                "status": "error", 
                "error": str(e)
            }
            
            try:
                # Ensure results stream exists for error response
                results_stream = f"{self.nats_config.stream_name}_results"
                try:
                    await self.client.js.stream_info(results_stream)
                except Exception as stream_e:
                    if "not found" in str(stream_e):
                        await self.client.js.add_stream(
                            name=results_stream,
                            subjects=[f"{self.nats_config.subject_prefix}.result.*"],
                            storage="memory",
                            retention="limits",
                            max_msgs=1000,
                            max_bytes=100 * 1024 * 1024,
                            max_age=3600
                        )
                
                await self.client.js.publish(
                    f"{self.nats_config.subject_prefix}.result.{request.get('request_id', 'unknown')}",
                    json.dumps(error_response).encode()
                )
            except:
                pass
                
            await message.nak()
    
    async def start_listening(self):
        """Start listening for processing requests"""
        print(f"🎧 Docling Worker: Listening for requests on '{self.nats_config.subject_prefix}.process.*'")
        
        # Ensure stream exists, create if needed
        try:
            await self.client.js.stream_info(self.nats_config.stream_name)
            print(f"✅ Stream {self.nats_config.stream_name} exists")
        except Exception as e:
            if "not found" in str(e):
                print(f"🔧 Creating missing stream: {self.nats_config.stream_name}")
                await self.client.js.add_stream(
                    name=self.nats_config.stream_name,
                    subjects=[f"{self.nats_config.subject_prefix}.process.*"],
                    storage="memory",
                    retention="workqueue",
                    max_msgs=1000,
                    max_bytes=100 * 1024 * 1024  # 100MB
                )
                print(f"✅ Created stream: {self.nats_config.stream_name}")
            else:
                raise
        
        # Subscribe to processing requests
        subscription = await self.client.js.pull_subscribe(
            subject=f"{self.nats_config.subject_prefix}.process.*",
            durable="docling_worker",
            stream=self.nats_config.stream_name
        )
        
        # Main processing loop
        processed_count = 0
        
        try:
            while True:
                try:
                    # Wait for messages
                    messages = await subscription.fetch(batch=1, timeout=10)
                    
                    if messages:
                        for message in messages:
                            await self.process_document_request(message)
                            processed_count += 1
                            print(f"📈 Docling Worker: Processed {processed_count} documents")
                    else:
                        print("⏱️  Docling Worker: No messages, waiting...")
                        
                except asyncio.TimeoutError:
                    print("⏱️  Docling Worker: Timeout, continuing...")
                    continue
                    
        except KeyboardInterrupt:
            print(f"\n👋 Docling Worker: Interrupted by user")
        finally:
            print(f"📊 Docling Worker: Final stats - Processed {processed_count} documents")
            await self.client.close()
            print("👋 Docling Worker: Shutting down")

async def main():
    """Main worker entry point"""
    print("🚀 Starting Docling Worker Service")
    print("=" * 50)
    
    worker = DoclingWorker()
    await worker.setup()
    await worker.start_listening()

if __name__ == "__main__":
    asyncio.run(main()) 