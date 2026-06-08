#!/usr/bin/env python3
"""
GPU Memory Configuration for Docling
Optimized presets for different GPU memory sizes and use cases
"""
import os
import torch
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class MemoryConfig:
    """Memory configuration settings"""
    memory_fraction: float
    max_batch_size: int
    vlm_batch_size: int  
    num_threads: int
    images_scale: float
    cleanup_threshold: float
    pytorch_alloc_conf: Dict[str, Any]
    enable_mixed_precision: bool
    gradient_checkpointing: bool
    description: str

class GPUMemoryOptimizer:
    """GPU Memory optimization configurations for different scenarios"""
    
    # Predefined configurations for different GPU memory sizes
    CONFIGS = {
        "24gb_conservative": MemoryConfig(
            memory_fraction=0.85,
            max_batch_size=2,
            vlm_batch_size=1,
            num_threads=4,
            images_scale=1.0,
            cleanup_threshold=0.75,
            pytorch_alloc_conf={
                'max_split_size_mb': 64,
                'garbage_collection_threshold': 0.7,
                'expandable_segments': True
            },
            enable_mixed_precision=True,
            gradient_checkpointing=True,
            description="Ultra-conservative for 24GB - prioritizes stability over speed"
        ),
        
        "24gb_balanced": MemoryConfig(
            memory_fraction=0.90,
            max_batch_size=4,
            vlm_batch_size=2,
            num_threads=6,
            images_scale=1.0,
            cleanup_threshold=0.80,
            pytorch_alloc_conf={
                'max_split_size_mb': 128,
                'garbage_collection_threshold': 0.8,
                'expandable_segments': True
            },
            enable_mixed_precision=True,
            gradient_checkpointing=False,
            description="Balanced performance and memory usage for 24GB"
        ),
        
        "24gb_performance": MemoryConfig(
            memory_fraction=0.95,
            max_batch_size=8,
            vlm_batch_size=4,
            num_threads=8,
            images_scale=1.5,
            cleanup_threshold=0.85,
            pytorch_alloc_conf={
                'max_split_size_mb': 256,
                'garbage_collection_threshold': 0.85,
                'expandable_segments': True
            },
            enable_mixed_precision=False,
            gradient_checkpointing=False,
            description="Maximum performance for 24GB - higher memory usage"
        ),
        
        "16gb_optimized": MemoryConfig(
            memory_fraction=0.85,
            max_batch_size=2,
            vlm_batch_size=1,
            num_threads=4,
            images_scale=0.8,
            cleanup_threshold=0.70,
            pytorch_alloc_conf={
                'max_split_size_mb': 32,
                'garbage_collection_threshold': 0.6,
                'expandable_segments': True
            },
            enable_mixed_precision=True,
            gradient_checkpointing=True,
            description="Optimized for 16GB GPUs"
        ),
        
        "12gb_minimal": MemoryConfig(
            memory_fraction=0.80,
            max_batch_size=1,
            vlm_batch_size=1,
            num_threads=2,
            images_scale=0.5,
            cleanup_threshold=0.65,
            pytorch_alloc_conf={
                'max_split_size_mb': 16,
                'garbage_collection_threshold': 0.5,
                'expandable_segments': True
            },
            enable_mixed_precision=True,
            gradient_checkpointing=True,
            description="Minimal memory usage for 12GB GPUs"
        ),

        # ~20GB GPU shared with Ollama (~10GB resident) — production NATS path A
        "20gb_nats": MemoryConfig(
            memory_fraction=0.45,
            max_batch_size=4,
            vlm_batch_size=2,
            num_threads=4,
            images_scale=1.0,
            cleanup_threshold=0.75,
            pytorch_alloc_conf={
                'max_split_size_mb': 64,
                'garbage_collection_threshold': 0.7,
                'expandable_segments': True
            },
            enable_mixed_precision=True,
            gradient_checkpointing=False,
            description="NATS Docling on 20GB GPU alongside Ollama (path A — full Docling GPU)"
        ),

        # Hard cap ~5GB for parser A/B benchmark path B (set_per_process_memory_fraction also applied)
        "capped_5gb": MemoryConfig(
            memory_fraction=0.25,
            max_batch_size=1,
            vlm_batch_size=1,
            num_threads=2,
            images_scale=0.8,
            cleanup_threshold=0.60,
            pytorch_alloc_conf={
                'max_split_size_mb': 16,
                'garbage_collection_threshold': 0.5,
                'expandable_segments': True
            },
            enable_mixed_precision=True,
            gradient_checkpointing=True,
            description="Cap Docling to ~5GB VRAM (path B — small batches, OCR/table only)"
        ),

        # Production shared GPU: 8GB Docling cap, batch=1, leave ~12GB for Ollama
        "20gb_capped": MemoryConfig(
            memory_fraction=0.40,
            max_batch_size=1,
            vlm_batch_size=1,
            num_threads=2,
            images_scale=0.5,
            cleanup_threshold=0.70,
            pytorch_alloc_conf={
                'max_split_size_mb': 128,
                'garbage_collection_threshold': 0.7,
                'expandable_segments': True
            },
            enable_mixed_precision=True,
            gradient_checkpointing=True,
            description="Production: 8GB Docling CUDA cap, batch=1, Ollama reserve via vram_policy"
        ),
    }
    
    @classmethod
    def apply_config(cls, config_name: str = "24gb_balanced") -> MemoryConfig:
        """Apply a predefined memory configuration"""
        
        if config_name not in cls.CONFIGS:
            available = ", ".join(cls.CONFIGS.keys())
            raise ValueError(f"Unknown config '{config_name}'. Available: {available}")
        
        config = cls.CONFIGS[config_name]
        
        print(f"🎛️ Applying GPU memory config: {config_name}")
        print(f"📝 Description: {config.description}")
        
        # Set PyTorch memory fraction
        if torch.cuda.is_available():
            torch.cuda.set_per_process_memory_fraction(config.memory_fraction)
            print(f"🔧 Memory fraction: {config.memory_fraction:.2%}")
        
        # Set PyTorch allocator configuration
        alloc_conf_str = ",".join([
            f"{k}:{v}" for k, v in config.pytorch_alloc_conf.items()
        ])
        os.environ['PYTORCH_CUDA_ALLOC_CONF'] = alloc_conf_str
        print(f"🔧 PyTorch alloc config: {alloc_conf_str}")
        
        # Set additional environment variables
        if config.enable_mixed_precision:
            os.environ['TORCH_ALLOW_TF32_CUBLAS_OVERRIDE'] = '1'
            print("🔧 Mixed precision enabled")
        
        # Memory debugging (optional)
        if config_name.endswith('_conservative') or config_name.endswith('_minimal'):
            os.environ['PYTORCH_NO_CUDA_MEMORY_CACHING'] = '0'  # Keep caching for better performance
            print("🔧 Conservative memory debugging enabled")
        
        return config
    
    @classmethod
    def get_optimal_docling_options(cls, config: MemoryConfig, user_options: Optional[Dict] = None) -> Dict[str, Any]:
        """Generate optimal Docling options based on memory configuration"""
        
        base_options = {
            'vlm_batch_size': config.vlm_batch_size,
            'images_scale': config.images_scale,
            'num_threads': config.num_threads,
            'layout_batch_size': 1,
            'ocr_batch_size': 1,
            'table_batch_size': 1,
            'queue_max_size': 1,
            'generate_picture_images': False,
            'generate_page_images': False,
            'force_full_page_ocr': False,
            'cuda_use_flash_attention2': not config.gradient_checkpointing,
        }

        if user_options:
            base_options.update(user_options)
            if user_options.get('accelerator_device', '').lower() in ('cpu', 'cuda', 'gpu'):
                pass
            elif 'accelerator_device' not in user_options:
                base_options.setdefault('accelerator_device', 'cuda')

            base_options['vlm_batch_size'] = min(
                base_options.get('vlm_batch_size', config.vlm_batch_size),
                config.max_batch_size,
            )
            print(
                f"🔧 User options merged, VLM batch size capped at "
                f"{base_options['vlm_batch_size']}"
            )

        return base_options
    
    @classmethod
    def detect_optimal_config(cls) -> str:
        """Auto-detect optimal configuration based on available GPU memory"""
        
        if not torch.cuda.is_available():
            print("❌ CUDA not available, using minimal config")
            return "12gb_minimal"
        
        # Get GPU memory in GB
        device = torch.cuda.current_device()
        total_memory_gb = torch.cuda.get_device_properties(device).total_memory / (1024**3)
        
        print(f"🔍 Detected GPU memory: {total_memory_gb:.1f}GB")
        
        # Select config based on memory size
        if total_memory_gb >= 22:  # 24GB cards
            return "24gb_balanced"
        elif total_memory_gb >= 15:  # 16GB cards  
            return "16gb_optimized"
        elif total_memory_gb >= 10:  # 12GB cards
            return "12gb_minimal"
        else:
            print("⚠️ Very low GPU memory detected, using minimal config")
            return "12gb_minimal"
    
    @classmethod
    def print_memory_status(cls):
        """Print current GPU memory status"""
        if not torch.cuda.is_available():
            print("❌ CUDA not available")
            return
        
        device = torch.cuda.current_device()
        allocated = torch.cuda.memory_allocated(device) / 1024**3
        reserved = torch.cuda.memory_reserved(device) / 1024**3  
        total = torch.cuda.get_device_properties(device).total_memory / 1024**3
        
        print(f"📊 GPU Memory Status:")
        print(f"   📍 Device: {torch.cuda.get_device_name(device)}")
        print(f"   💾 Total: {total:.2f}GB")
        print(f"   🔄 Reserved: {reserved:.2f}GB ({reserved/total*100:.1f}%)")
        print(f"   ✅ Allocated: {allocated:.2f}GB ({allocated/total*100:.1f}%)")
        print(f"   🆓 Free: {total-reserved:.2f}GB ({(total-reserved)/total*100:.1f}%)")

# Convenience function for easy usage
def setup_gpu_optimization(config_name: Optional[str] = None, user_options: Optional[Dict] = None) -> Dict[str, Any]:
    """
    One-line setup for GPU optimization
    
    Args:
        config_name: Configuration preset name (auto-detected if None)
        user_options: User-specific Docling options to merge
    
    Returns:
        Optimized Docling options dictionary
    """
    
    # Auto-detect or use provided config
    if config_name is None:
        config_name = GPUMemoryOptimizer.detect_optimal_config()
    
    # Apply configuration
    config = GPUMemoryOptimizer.apply_config(config_name)
    
    # Get optimized options
    docling_options = GPUMemoryOptimizer.get_optimal_docling_options(config, user_options)
    
    # Print status
    GPUMemoryOptimizer.print_memory_status()
    
    print(f"✅ GPU optimization setup complete!")
    print(f"🎯 Recommended options: {list(docling_options.keys())}")
    
    return docling_options

# Example usage configurations
EXAMPLE_CONFIGS = {
    "vlm_heavy": {
        "do_picture_description": True,
        "vlm_model": "smolvlm",  # Prefer smaller VLM models
        "custom_prompt": "Describe this image concisely.",
        "do_picture_classification": False,  # Disable to save memory
    },
    
    "text_extraction": {
        "do_picture_description": False,
        "do_ocr": True,
        "do_table_structure": True,
        "force_full_page_ocr": False,  # Conservative
    },
    
    "full_processing": {
        "do_picture_description": True,
        "vlm_model": "smolvlm",
        "do_picture_classification": True,
        "do_ocr": True,
        "do_table_structure": True,
        "do_code_enrichment": True,
        "do_formula_enrichment": True,
    }
}

if __name__ == "__main__":
    # Demo the configuration system
    print("🚀 GPU Memory Configuration Demo")
    print("=" * 50)
    
    # Show available configs
    print("\n📋 Available configurations:")
    for name, config in GPUMemoryOptimizer.CONFIGS.items():
        print(f"  • {name}: {config.description}")
    
    # Auto-detect and setup
    print(f"\n🔍 Auto-detection:")
    optimal_config = GPUMemoryOptimizer.detect_optimal_config()
    print(f"   Recommended: {optimal_config}")
    
    # Setup with VLM processing
    print(f"\n🎛️ Setting up for VLM processing:")
    options = setup_gpu_optimization(
        config_name=optimal_config,
        user_options=EXAMPLE_CONFIGS["vlm_heavy"]
    )
    
    print(f"\n🎯 Final Docling options:")
    for key, value in options.items():
        print(f"   {key}: {value}") 