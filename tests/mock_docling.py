"""Mock docling module for testing"""

class Client:
    """Mock Docling client"""
    
    async def process_document(self, document_data, options=None):
        """Mock document processing"""
        # Simulate processing based on document size
        text = f"Extracted text from {len(document_data)} byte document"
        
        return {
            "text": text,
            "metadata": {
                "pages": max(1, len(document_data) // 1000),
                "title": "Test Document",
                "author": "Test Author"
            },
            "images": [],
            "tables": []
        } 