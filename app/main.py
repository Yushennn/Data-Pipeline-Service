from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from typing import Annotated, List, Optional
#--------------------------- Data Schema Definitions ---------------------------

# # Define single data record format (a single row of data)
# class DataRecord(BaseModel):
#     id: int
#     value: float
#     category: str
    
# # Define batch data format (multiple rows of data)
# class ProcessingRequest(BaseModel):
#     dataset_name: str = Field(..., description="Name of the dataset")
#     records: List[DataRecord] = Field(..., description="List of data records to process")
#     config: Optional[Dict[str, Any]] = Field(default=None, description="Optional processing parameters")
    
# # Define processing result format
# class ProcessingResponse(BaseModel):
#     status: str
#     processed_count: int
#     message: str
    
# --------------------------- FastAPI Application Setup ---------------------------


# intialize FastAPI app
app = FastAPI(
    title = "Data Processing Pipeline API",
    description = "Backend service for data ingestion and preprocessing.",
    version = "1.0.0"
)

# Health check endpoint
@app.get("/status",tags = ["System"])
def check_status():
    return {
        "status": "success",
        "message": "API server is running and healthy.",
        "service": "Data Processing Pipeline API"
    }

# Data Ingestion endpoint for uploading datasets
@app.post("/upload-dataset", tags = ["Data Processing"])
async def upload_datasets(files: Annotated[List[UploadFile], File(description="Upload multiple CSV/Excel files")],
                          
                          weight_class_M58: Annotated[float, Form(description="Average Match Time of Weight M58 for datasets")],
                          weight_class_M68: Annotated[float, Form(description="Average Match Time of Weight M68 for datasets")],
                          weight_class_M80: Annotated[float, Form(description="Average Match Time of Weight M80 for datasets")],
                          weight_class_M80p: Annotated[float, Form(description="Average Match Time of Weight M80+ for datasets")],
                          ):
    
    file_info = []
    for file in files:
        if not file.filename.endswith(('.csv', '.xlsx')):
            raise HTTPException(status_code=400, detail=f"Unsupported file: {file.filename}")
        
        file_info.append({
            "filename": file.filename,
            "content_type": file.content_type
        })
    
    
    return {
        "status": "success",
        "metadata": {
            "weight_class_M58": weight_class_M58,
            "weight_class_M68": weight_class_M68,
            "weight_class_M80": weight_class_M80,
            "weight_class_M80p": weight_class_M80p
        },
        "uploaded_files": file_info,
        "total_files": len(file_info),
        "message": "Files received successfully."
    }