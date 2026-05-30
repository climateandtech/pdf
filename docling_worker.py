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

from s3_client import S3DocumentClient
from s3_config import S3Config
from config import NatsConfig
from worker_runtime import bootstrap_gpu, cleanup_gpu_memory

logger = logging.getLogger(__name__)

class DoclingWorker:
    """Docling processing worker that communicates via NATS"""
    
    def __init__(self):
        # S3 + NATS client
        self.s3_config = S3Config()
        self.nats_config = NatsConfig()
        self.client = S3DocumentClient(self.s3_config, self.nats_config)

        # DocumentConverter is created per request (after bootstrap_gpu loads torch/docling)
        print("🤖 Docling Worker: Dynamic VLM configuration enabled")
        print("📋 VLM options will be specified by the publisher for each request")
        
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
            from docling.document_converter import DocumentConverter

            return DocumentConverter()

        try:
            from memory_patch import get_memory_optimized_options

            print(f"🎛️  Configuring DocumentConverter with options: {list(docling_options.keys())}")
            docling_options = get_memory_optimized_options(docling_options)
            
            # Check if we have simple JSON options (sent over NATS)
            if self._is_simple_options(docling_options):
                print("🔄 Converting simple options to Docling objects...")
                docling_config = self._convert_simple_options(docling_options)
            else:
                print("📋 Using provided Docling configuration objects...")
                docling_config = docling_options
            
            # Create DocumentConverter with converted options
            from docling.document_converter import DocumentConverter

            converter = DocumentConverter(**docling_config)
            
            print("✅ DocumentConverter configured successfully")
            return converter
            
        except Exception as e:
            print(f"⚠️  DocumentConverter configuration failed: {e}")
            print("🔄 Falling back to standard converter")
            from docling.document_converter import DocumentConverter

            return DocumentConverter()
    
    def _is_simple_options(self, options):
        """Check if options are simple JSON options (vs complex Docling objects)"""
        if not isinstance(options, dict):
            return False
        
        # Simple options cover ONLY REAL Docling pipeline settings (verified against API)
        simple_keys = {
            # === VLM OPTIONS (REAL) ===
            'vlm_model', 'do_picture_description', 'images_scale', 'custom_prompt', 'vlm_prompt',
            'vlm_batch_size', 'vlm_picture_area_threshold', 'vlm_generation_config',
            
            # === ENRICHMENT OPTIONS (REAL) ===  
            'do_picture_classification', 'do_code_enrichment', 'do_formula_enrichment',
            'do_table_structure', 'do_ocr',
            
            # === OCR OPTIONS (REAL) ===
            'ocr_languages', 'force_full_page_ocr', 'ocr_bitmap_area_threshold', 
            'ocr_use_gpu', 'ocr_confidence_threshold', 'ocr_model_storage_directory',
            'ocr_recog_network', 'ocr_download_enabled',
            
            # === TABLE STRUCTURE OPTIONS (REAL) ===
            'table_do_cell_matching', 'table_mode',
            
            # === IMAGE & PAGE OPTIONS (REAL) ===
            'generate_picture_images', 'generate_page_images', 'generate_table_images',
            
            # === CORE PIPELINE OPTIONS (REAL) ===
            'create_legacy_output', 'document_timeout', 'enable_remote_services',
            'allow_external_plugins', 'artifacts_path', 'force_backend_text',
            'generate_parsed_pages',
            
            # === PERFORMANCE OPTIONS (REAL) ===
            'accelerator_device', 'num_threads', 'cuda_use_flash_attention2',
            
            # === INPUT FORMAT SUPPORT ===
            'input_formats'
        }
        complex_keys = {'format_options', 'accelerator_options'}
        
        has_simple = any(key in options for key in simple_keys)
        has_complex = any(key in options for key in complex_keys)
        
        return has_simple and not has_complex
    
    def _convert_simple_options(self, simple_options):
        """Convert simple JSON options to proper Docling objects"""
        from docling.document_converter import PdfFormatOption
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import (
            PdfPipelineOptions, 
            AcceleratorOptions,
            AcceleratorDevice,
            OcrEngine,
            granite_picture_description, 
            smolvlm_picture_description
        )
        
        # Start with basic pipeline options
        pipeline_options = PdfPipelineOptions()
        
        # ======================
        # CORE PIPELINE OPTIONS (REAL Docling fields)
        # ======================
        if 'create_legacy_output' in simple_options:
            pipeline_options.create_legacy_output = bool(simple_options['create_legacy_output'])
            print(f"📜 Create legacy output: {pipeline_options.create_legacy_output}")
        
        if 'document_timeout' in simple_options:
            pipeline_options.document_timeout = float(simple_options['document_timeout']) if simple_options['document_timeout'] else None
            print(f"⏱️  Document timeout: {pipeline_options.document_timeout}")
        
        if 'enable_remote_services' in simple_options:
            pipeline_options.enable_remote_services = bool(simple_options['enable_remote_services'])
            print(f"🌐 Remote services: {pipeline_options.enable_remote_services}")
        
        if 'allow_external_plugins' in simple_options:
            pipeline_options.allow_external_plugins = bool(simple_options['allow_external_plugins'])
            print(f"🔌 External plugins: {pipeline_options.allow_external_plugins}")
        
        if 'force_backend_text' in simple_options:
            pipeline_options.force_backend_text = bool(simple_options['force_backend_text'])
            print(f"📝 Force backend text: {pipeline_options.force_backend_text}")
        
        if 'generate_parsed_pages' in simple_options:
            pipeline_options.generate_parsed_pages = bool(simple_options['generate_parsed_pages'])
            print(f"📄 Generate parsed pages: {pipeline_options.generate_parsed_pages}")
        
        if 'artifacts_path' in simple_options:
            pipeline_options.artifacts_path = simple_options['artifacts_path']
            print(f"📁 Artifacts path: {pipeline_options.artifacts_path}")
            
        # ======================
        # IMAGE & PAGE OPTIONS (REAL Docling fields)
        # ======================
        if 'generate_page_images' in simple_options:
            pipeline_options.generate_page_images = bool(simple_options['generate_page_images'])
            print(f"🖼️  Generate page images: {pipeline_options.generate_page_images}")
        
        if 'generate_table_images' in simple_options:
            pipeline_options.generate_table_images = bool(simple_options['generate_table_images'])
            print(f"📊 Generate table images: {pipeline_options.generate_table_images}")
        
        # ======================
        # VLM PICTURE DESCRIPTION
        # ======================
        if simple_options.get('do_picture_description', False):
            pipeline_options.do_picture_description = True
            
            # Set image scale
            pipeline_options.images_scale = simple_options.get('images_scale', 2.0)
            
            # Check if custom prompt is provided
            custom_prompt = simple_options.get('custom_prompt') or simple_options.get('vlm_prompt')
            
            if custom_prompt:
                # Use PictureDescriptionVlmOptions for custom prompts
                from docling.datamodel.pipeline_options import PictureDescriptionVlmOptions
                
                vlm_model = simple_options.get('vlm_model', 'granite').lower()
                if vlm_model == 'granite':
                    repo_id = "ibm-granite/granite-vision-3.1-2b-preview"
                elif vlm_model == 'smoldocling' or vlm_model == 'smolvlm':
                    repo_id = "HuggingFaceTB/SmolVLM-256M-Instruct"
                else:
                    repo_id = "ibm-granite/granite-vision-3.1-2b-preview"
                
                vlm_options = PictureDescriptionVlmOptions(
                    repo_id=repo_id,
                    prompt=custom_prompt
                )
                
                # Set additional VLM options if provided
                if 'vlm_batch_size' in simple_options:
                    vlm_options.batch_size = int(simple_options['vlm_batch_size'])
                    print(f"📦 VLM batch size: {vlm_options.batch_size}")
                
                if 'vlm_picture_area_threshold' in simple_options:
                    vlm_options.picture_area_threshold = float(simple_options['vlm_picture_area_threshold'])
                    print(f"📏 VLM area threshold: {vlm_options.picture_area_threshold}")
                
                if 'vlm_generation_config' in simple_options:
                    vlm_options.generation_config = simple_options['vlm_generation_config']
                    print(f"⚙️  VLM generation config: {vlm_options.generation_config}")
                
                pipeline_options.picture_description_options = vlm_options
                print(f"🤖 Using {vlm_model} with custom prompt: {custom_prompt[:50]}...")
                
            else:
                # Use pre-configured models without custom prompts
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
        
        # ======================
        # PICTURE CLASSIFICATION
        # ======================
        if simple_options.get('do_picture_classification', False):
            pipeline_options.do_picture_classification = True
            print("📊 Enabling picture classification")
        
        # ======================
        # CODE ENRICHMENT
        # ======================
        if simple_options.get('do_code_enrichment', False):
            pipeline_options.do_code_enrichment = True
            print("💻 Enabling code enrichment")
        
        # ======================
        # FORMULA ENRICHMENT
        # ======================
        if simple_options.get('do_formula_enrichment', False):
            pipeline_options.do_formula_enrichment = True
            print("🧮 Enabling formula enrichment")
        
        # ======================
        # OCR OPTIONS (ALL REAL Docling OCR fields)
        # ======================
        if 'do_ocr' in simple_options:
            pipeline_options.do_ocr = bool(simple_options['do_ocr'])
            print(f"🔍 OCR: {pipeline_options.do_ocr}")
        
        # Advanced OCR Options - customize ocr_options if any OCR settings are provided
        if any(key.startswith('ocr_') for key in simple_options.keys()) or 'force_full_page_ocr' in simple_options:
            # Start with existing OCR options (which have defaults) and modify them
            ocr_options = pipeline_options.ocr_options
            
            if 'ocr_languages' in simple_options:
                langs = simple_options['ocr_languages']
                if isinstance(langs, str):
                    langs = [langs]
                ocr_options.lang = langs
                print(f"🌍 OCR languages: {langs}")
            
            if 'force_full_page_ocr' in simple_options:
                ocr_options.force_full_page_ocr = bool(simple_options['force_full_page_ocr'])
                print(f"🔍 Force full page OCR: {ocr_options.force_full_page_ocr}")
            
            if 'ocr_bitmap_area_threshold' in simple_options:
                ocr_options.bitmap_area_threshold = float(simple_options['ocr_bitmap_area_threshold'])
                print(f"📏 OCR bitmap threshold: {ocr_options.bitmap_area_threshold}")
            
            if 'ocr_use_gpu' in simple_options:
                ocr_options.use_gpu = bool(simple_options['ocr_use_gpu']) if simple_options['ocr_use_gpu'] is not None else None
                print(f"🚀 OCR use GPU: {ocr_options.use_gpu}")
            
            if 'ocr_confidence_threshold' in simple_options:
                ocr_options.confidence_threshold = float(simple_options['ocr_confidence_threshold'])
                print(f"🎯 OCR confidence: {ocr_options.confidence_threshold}")
            
            if 'ocr_model_storage_directory' in simple_options:
                ocr_options.model_storage_directory = simple_options['ocr_model_storage_directory']
                print(f"📁 OCR model dir: {ocr_options.model_storage_directory}")
            
            if 'ocr_recog_network' in simple_options:
                ocr_options.recog_network = simple_options['ocr_recog_network']
                print(f"🧠 OCR network: {ocr_options.recog_network}")
            
            if 'ocr_download_enabled' in simple_options:
                ocr_options.download_enabled = bool(simple_options['ocr_download_enabled'])
                print(f"📥 OCR download: {ocr_options.download_enabled}")
            
            pipeline_options.ocr_options = ocr_options
        
        # ======================
        # TABLE STRUCTURE (ALL REAL Docling table options)
        # ======================
        if 'do_table_structure' in simple_options:
            pipeline_options.do_table_structure = bool(simple_options['do_table_structure'])
            print(f"📋 Table structure: {pipeline_options.do_table_structure}")
        
        # Advanced Table Structure Options
        if 'table_do_cell_matching' in simple_options or 'table_mode' in simple_options:
            from docling.datamodel.pipeline_options import TableStructureOptions
            table_options = TableStructureOptions()
            
            if 'table_do_cell_matching' in simple_options:
                table_options.do_cell_matching = bool(simple_options['table_do_cell_matching'])
                print(f"🔗 Table cell matching: {table_options.do_cell_matching}")
            
            if 'table_mode' in simple_options:
                # Handle table mode (need to import TableFormerMode if available)
                try:
                    from docling.datamodel.pipeline_options import TableFormerMode
                    mode_name = simple_options['table_mode'].upper()
                    if hasattr(TableFormerMode, mode_name):
                        table_options.mode = getattr(TableFormerMode, mode_name)
                        print(f"📊 Table mode: {mode_name}")
                    else:
                        print(f"⚠️  Unknown table mode: {mode_name}")
                except ImportError:
                    print("⚠️  TableFormerMode not available")
            
            pipeline_options.table_structure_options = table_options
        
        # ======================
        # IMAGE GENERATION
        # ======================
        if simple_options.get('generate_picture_images', True):  # Default enabled
            pipeline_options.generate_picture_images = True
        
        # ======================
        # ADDITIONAL PIPELINE OPTIONS
        # ======================
        # Note: Some advanced options like chunking may not be available in all Docling versions
        

        
        # ======================
        # ASR (AUDIO) OPTIONS
        # ======================
        if simple_options.get('do_asr', False):
            from docling.datamodel.pipeline_options import AudioPipelineOptions
            
            # Create audio pipeline options
            audio_pipeline_options = AudioPipelineOptions()
            
            # ASR model selection
            asr_model = simple_options.get('asr_model', 'whisper_tiny').lower()
            # Map common names to actual model names
            asr_model_map = {
                'whisper': 'whisper_tiny',
                'whisper_tiny': 'whisper_tiny',
                'whisper_small': 'whisper_small',
                'whisper_base': 'whisper_base',
                'whisper_large': 'whisper_large'
            }
            audio_pipeline_options.asr_model = asr_model_map.get(asr_model, 'whisper_tiny')
            
            # Language setting
            asr_language = simple_options.get('asr_language', 'auto')
            if asr_language != 'auto':
                audio_pipeline_options.language = asr_language
            
            print(f"🎙️  Enabling ASR with model: {audio_pipeline_options.asr_model}")
            if asr_language != 'auto':
                print(f"   Language: {asr_language}")

        # ======================
        # FORMAT OPTIONS
        # ======================
        # Start with PDF as default
        format_options = {
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
        
        # Add additional input formats if specified
        input_formats = simple_options.get('input_formats', ['pdf'])
        if not isinstance(input_formats, list):
            input_formats = [input_formats]
        
        for fmt in input_formats:
            fmt_lower = fmt.lower()
            if fmt_lower in ['docx', 'doc']:
                from docling.document_converter import WordFormatOption
                format_options[InputFormat.DOCX] = WordFormatOption()
                print(f"📄 Added support for: {fmt_lower.upper()}")
            elif fmt_lower in ['image', 'png', 'jpg', 'jpeg']:
                from docling.document_converter import ImageFormatOption
                format_options[InputFormat.IMAGE] = ImageFormatOption()
                print(f"🖼️  Added support for: {fmt_lower.upper()}")
            elif fmt_lower in ['html', 'htm']:
                from docling.document_converter import HTMLFormatOption
                format_options[InputFormat.HTML] = HTMLFormatOption()
                print(f"🌐 Added support for: {fmt_lower.upper()}")
            elif fmt_lower in ['pptx', 'ppt']:
                from docling.document_converter import PowerpointFormatOption
                format_options[InputFormat.PPTX] = PowerpointFormatOption()
                print(f"📊 Added support for: {fmt_lower.upper()}")
        
        # Add audio formats if ASR is enabled
        if simple_options.get('do_asr', False):
            from docling.document_converter import AudioFormatOption
            format_options[InputFormat.AUDIO] = AudioFormatOption(
                pipeline_options=audio_pipeline_options
            )
            print("🎙️  Added support for: AUDIO")
        
        # ======================
        # ACCELERATOR OPTIONS
        # ======================
        converter_options = {"format_options": format_options}
        
        # Add accelerator options if specified
        accelerator_device = simple_options.get('accelerator_device')
        num_threads = simple_options.get('num_threads')
        
        if accelerator_device or num_threads or simple_options.get('cuda_use_flash_attention2') is not None:
            device = AcceleratorDevice.AUTO  # Default
            if accelerator_device:
                device_map = {
                    'cpu': AcceleratorDevice.CPU,
                    'gpu': AcceleratorDevice.CUDA,
                    'cuda': AcceleratorDevice.CUDA,
                    'mps': AcceleratorDevice.MPS,
                    'auto': AcceleratorDevice.AUTO
                }
                device = device_map.get(accelerator_device.lower(), AcceleratorDevice.AUTO)
                print(f"🚀 Using accelerator: {accelerator_device}")
            
            # Build accelerator options
            accel_kwargs = {
                'num_threads': num_threads or 4,
                'device': device
            }
            
            # Add CUDA flash attention if specified
            if 'cuda_use_flash_attention2' in simple_options:
                accel_kwargs['cuda_use_flash_attention2'] = bool(simple_options['cuda_use_flash_attention2'])
                print(f"⚡ CUDA Flash Attention 2: {accel_kwargs['cuda_use_flash_attention2']}")
            
            accelerator_options = AcceleratorOptions(**accel_kwargs)
            
            # Set accelerator options on the pipeline (this is the correct way)
            pipeline_options.accelerator_options = accelerator_options
            print(f"⚡ Accelerator configured: {device}, threads: {num_threads or 4}")
        
        # Note: artifacts_path is already handled in CORE PIPELINE OPTIONS section above
        
        # ======================
        # CORE DOCUMENTCONVERTER OPTIONS
        # ======================
        # Timeout settings
        timeout = simple_options.get('timeout')
        if timeout:
            converter_options["timeout"] = float(timeout)
            print(f"⏱️  Setting timeout: {timeout}s")
        
        # Error handling
        raises_on_error = simple_options.get('raises_on_error')
        if raises_on_error is not None:
            converter_options["raises_on_error"] = bool(raises_on_error)
            print(f"🛡️  Raises on error: {raises_on_error}")
        
        # Debug mode
        debug_mode = simple_options.get('debug_mode', False)
        if debug_mode:
            converter_options["debug_mode"] = True
            print("🐛 Debug mode enabled")
        
        # File size limits
        max_file_size = simple_options.get('max_file_size')
        if max_file_size:
            converter_options["max_file_size"] = int(max_file_size)
            print(f"📏 Max file size: {max_file_size} bytes")
        
        # Note: Output/export options may not be DocumentConverter parameters
        # These would typically be handled at the document export level
        
        return converter_options

    async def process_document_request(self, message):
        """Process a document processing request from NATS"""
        request_id = "unknown"
        temp_file: Path | None = None
        request: dict = {}
        try:
            # Parse the request
            request = json.loads(message.data.decode())
            request_id = request.get("request_id")
            s3_key = request.get("s3_key")
            backend_resource_id = request.get("backend_resource_id")
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
                "backend_resource_id": backend_resource_id,
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
            
            # Send response back via NATS (DOCUMENTS stream — same as platform-backend)
            await self.client.js.publish(
                f"{self.nats_config.subject_prefix}.result.{request_id}",
                json.dumps(response).encode()
            )
            
            print(f"📤 Docling Worker: Sent response for {request_id}")
            
            # Acknowledge the message
            await message.ack()

        except Exception as e:
            print(f"❌ Docling Worker: Error processing request: {e}")
            cleanup_gpu_memory(force=True)

            error_response = {
                "request_id": request_id,
                "status": "error",
                "backend_resource_id": request.get("backend_resource_id"),
                "error": str(e),
            }

            try:
                await self.client.js.publish(
                    f"{self.nats_config.subject_prefix}.result.{request_id}",
                    json.dumps(error_response).encode(),
                )
            except Exception:
                pass

            await message.nak()
        finally:
            if temp_file is not None and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            cleanup_gpu_memory(force=False)

    async def start_listening(self):
        """Start listening for processing requests"""
        print(f"🎧 Docling Worker: Listening for requests on '{self.nats_config.subject_prefix}.process.*'")
        
        # Stream must exist on broker with docs.process.* (see config/nats_streams.yaml).
        # Do not auto-create a workqueue-only stream — it breaks multi-consumer docs.result.
        try:
            info = await self.client.js.stream_info(self.nats_config.stream_name)
            subjects = list(info.config.subjects or [])
            need = f"{self.nats_config.subject_prefix}.process.*"
            if not any(
                s in (need, f"{self.nats_config.subject_prefix}.>", "docs.>")
                for s in subjects
            ):
                print(
                    f"⚠️  Stream {self.nats_config.stream_name} subjects={subjects} "
                    f"may not capture {need}; run fix_nats_documents_stream.py on broker"
                )
            else:
                print(f"✅ Stream {self.nats_config.stream_name} exists: {subjects}")
        except Exception as e:
            if "not found" in str(e).lower():
                raise RuntimeError(
                    f"JetStream stream {self.nats_config.stream_name} missing — "
                    "run coolify-provisioning/scripts/fix_nats_documents_stream.py"
                ) from e
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
                except Exception as loop_err:
                    from nats.errors import ConnectionClosedError

                    if isinstance(loop_err, ConnectionClosedError):
                        print(f"⚠️  NATS connection closed: {loop_err} — reconnecting...")
                        await self.client.close()
                        await self.client.setup()
                        subscription = await self.client.js.pull_subscribe(
                            subject=f"{self.nats_config.subject_prefix}.process.*",
                            durable="docling_worker",
                            stream=self.nats_config.stream_name,
                        )
                        continue
                    raise
                    
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

    profile = bootstrap_gpu()
    from memory_patch import setup_memory_optimization

    setup_memory_optimization(profile)

    # Warm up docling after torch/torchvision (avoid per-request circular import)
    from docling.document_converter import DocumentConverter

    DocumentConverter()

    worker = DoclingWorker()
    await worker.setup()
    await worker.start_listening()

if __name__ == "__main__":
    asyncio.run(main()) 