#!/usr/bin/env python3
"""
Memory Optimization Patch for Existing Docling Worker

Simple integration patch that adds GPU memory optimization to your existing docling_worker.py
Just import and call setup_memory_optimization() at the start of your worker.
"""
import os
import torch
import gc
import logging
from typing import Dict, Optional, Any
from gpu_memory_config import setup_gpu_optimization, GPUMemoryOptimizer

logger = logging.getLogger(__name__)

class MemoryOptimizationPatch:
    """Memory optimization patches for existing Docling worker"""
    
    def __init__(self, config_name: str = "24gb_balanced"):
        self.config_name = config_name
        self.cleanup_counter = 0
        self.cleanup_interval = 5  # Clean up every 5 documents
        
    def setup_optimization(self):
        """Setup GPU memory optimization (call this in your worker's __init__)"""
        
        print("🚀 Applying memory optimization patch...")
        
        # Apply GPU configuration
        try:
            self.config = GPUMemoryOptimizer.apply_config(self.config_name)
            print(f"✅ Memory optimization applied: {self.config_name}")
        except Exception as e:
            print(f"⚠️ Memory optimization setup failed: {e}")
            print("🔄 Continuing with default settings...")
        
    def get_optimized_options(self, user_options: Optional[Dict] = None) -> Dict[str, Any]:
        """Get memory-optimized Docling options"""
        
        # Use the configuration system
        try:
            optimized = setup_gpu_optimization(self.config_name, user_options)
            print(f"🎯 Generated optimized options with {len(optimized)} settings")
            return optimized
        except Exception as e:
            print(f"⚠️ Failed to generate optimized options: {e}")
            # Fallback to safe defaults
            return self._get_safe_defaults(user_options)
    
    def _get_safe_defaults(self, user_options: Optional[Dict] = None) -> Dict[str, Any]:
        """Safe fallback defaults for 24GB VRAM"""
        
        defaults = {
            'vlm_batch_size': 2,
            'images_scale': 1.0,
            'num_threads': 4,
            'accelerator_device': 'gpu',
            'generate_picture_images': False,
            'generate_page_images': False,
        }
        
        if user_options:
            defaults.update(user_options)
            # Enforce memory limits
            defaults['vlm_batch_size'] = min(defaults.get('vlm_batch_size', 2), 4)
        
        return defaults
    
    def cleanup_memory(self, force: bool = False):
        """Clean up GPU memory (call this after processing documents)"""
        
        self.cleanup_counter += 1
        
        if force or self.cleanup_counter >= self.cleanup_interval:
            print("🧹 Running memory cleanup...")
            
            # Clear Python garbage
            collected = gc.collect()
            
            # Clear PyTorch cache
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                torch.cuda.synchronize()
            
            # Force another garbage collection
            gc.collect()
            
            self.cleanup_counter = 0
            print(f"✅ Memory cleanup complete: {collected} objects collected")
            
            # Print memory status
            self._print_memory_status()
    
    def _print_memory_status(self):
        """Print current memory status"""
        if torch.cuda.is_available():
            device = torch.cuda.current_device()
            allocated = torch.cuda.memory_allocated(device) / 1024**3
            reserved = torch.cuda.memory_reserved(device) / 1024**3
            total = torch.cuda.get_device_properties(device).total_memory / 1024**3
            free = total - reserved
            
            print(f"📊 GPU Memory: {allocated:.1f}GB allocated, {reserved:.1f}GB reserved, {free:.1f}GB free")
    
    def monitor_memory_usage(self):
        """Check if memory usage is getting high"""
        if not torch.cuda.is_available():
            return False
            
        device = torch.cuda.current_device()
        total_memory = torch.cuda.get_device_properties(device).total_memory
        reserved_memory = torch.cuda.memory_reserved(device)
        utilization = reserved_memory / total_memory
        
        if utilization > 0.85:  # 85% threshold
            print(f"⚠️ High memory usage detected: {utilization:.1%}")
            self.cleanup_memory(force=True)
            return True
        
        return False

# Global instance for easy integration
memory_optimizer = MemoryOptimizationPatch()

# Simple integration functions that can be called from existing code
def setup_memory_optimization(config_name: str = "24gb_balanced"):
    """Setup memory optimization (call once at startup)"""
    global memory_optimizer
    memory_optimizer = MemoryOptimizationPatch(config_name)
    memory_optimizer.setup_optimization()

def get_memory_optimized_options(user_options: Optional[Dict] = None) -> Dict[str, Any]:
    """Get memory-optimized Docling options"""
    return memory_optimizer.get_optimized_options(user_options)

def cleanup_gpu_memory(force: bool = False):
    """Clean up GPU memory"""
    memory_optimizer.cleanup_memory(force)

def check_memory_usage() -> bool:
    """Check and cleanup if memory usage is high"""
    return memory_optimizer.monitor_memory_usage()

# Integration example for existing docling_worker.py
INTEGRATION_EXAMPLE = '''
# Add these imports at the top of your docling_worker.py:
from memory_patch import setup_memory_optimization, get_memory_optimized_options, cleanup_gpu_memory, check_memory_usage

class DoclingWorker:
    def __init__(self):
        # ADD THIS LINE - setup memory optimization
        setup_memory_optimization("24gb_balanced")  # or "24gb_conservative" for safer operation
        
        # ... your existing initialization code ...
        
    def _create_document_converter(self, docling_options=None):
        """Your existing method with memory optimization"""
        
        # ADD THIS LINE - get optimized options
        optimized_options = get_memory_optimized_options(docling_options)
        
        # Use optimized_options instead of docling_options
        if self._is_simple_options(optimized_options):
            docling_config = self._convert_simple_options(optimized_options)
        else:
            docling_config = optimized_options
            
        return DocumentConverter(**docling_config)
    
    async def process_document(self, s3_key: str, docling_options: Optional[Dict] = None):
        """Your existing method with memory monitoring"""
        
        # ADD THIS LINE - check memory before processing
        check_memory_usage()
        
        try:
            # ... your existing processing code ...
            
            # ADD THIS LINE - cleanup after processing
            cleanup_gpu_memory()
            
            return result
            
        except Exception as e:
            # ADD THIS LINE - emergency cleanup on error
            cleanup_gpu_memory(force=True)
            raise e
'''

if __name__ == "__main__":
    print("🚀 Memory Optimization Patch")
    print("=" * 50)
    print("This patch provides simple memory optimization for existing Docling workers.")
    print("\nTo integrate into your existing docling_worker.py:")
    print(INTEGRATION_EXAMPLE) 