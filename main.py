import time
from typing import List
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
import aiohttp
import aiofiles
import io
from PIL import Image
from fastapi import FastAPI, HTTPException, UploadFile, BackgroundTasks
from fastapi.responses import JSONResponse
from uuid import uuid4
import csv
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_200_OK
import boto3
import requests
import uvicorn
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(".env")

# Initialize FastAPI app
app = FastAPI()

# Database connection details
db_url = "mongodb://localhost:27017"
client = AsyncIOMotorClient(db_url)
db = client.process_image
requests_coll = db["requests"]
items_coll = db["items"]

# Response model for upload endpoint
class UploadResponse(BaseModel):
    request_id: str

# Response model for status endpoint
class StatusResponse(BaseModel):
    request_id: str
    status: str
    compressed_images: list

class AWSS3:
    """
    Class to handle AWS S3 interactions
    """
    def __init__(self) -> None:
        self.S3_BUCKET = os.getenv("S3_BUCKET")
        self.S3_KEY = os.getenv("S3_KEY")
        self.S3_SECRET = os.getenv("S3_SECRET")
        self.S3_LOCATION = os.getenv("S3_LOCATION")
        self.REGION_NAME = os.getenv('REGION_NAME')

    def upload_file_aws(self, byte_data, file_name):
        """
        Uploads a file to AWS S3 and returns the file URL
        """
        try:
            client = boto3.client('s3', region_name=self.REGION_NAME, 
                                  aws_access_key_id=self.S3_KEY, 
                                  aws_secret_access_key=self.S3_SECRET)
            
            fields = {'acl': 'public-read'}
            conditions = [{"acl": "public-read"}]
            fields['Content-Type'] = "image/png"
            conditions.append({"Content-Type": "image/png"})
            
            buc_address = "Bucket Address"
            boto3_response = client.generate_presigned_post(Bucket=self.S3_BUCKET, 
                                                            Key=buc_address, 
                                                            Fields=fields, 
                                                            ExpiresIn=60, 
                                                            Conditions=conditions)
            
            files = {'file': (file_name, byte_data)}
            data = boto3_response['fields']
            r = requests.post(boto3_response['url'], data=data, files=files)

            if r.status_code != 200:
                return ""
            
            file_url = self.S3_LOCATION + buc_address
            return file_url
        except Exception as e:
            return ""

async def compress_image(url: str) -> bytes:
    """
    Downloads an image from a URL, compresses it to 50% quality, and returns the compressed image bytes
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url) as response:
            if response.status == 200:
                img_bytes = await response.read()
                img = Image.open(io.BytesIO(img_bytes))
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=50)
                buf.seek(0)
                return buf.read()

async def process_image(request_id: str, items_list: List[str]):
    """
    Processes the images for a given request by compressing them and storing the results in the database
    """
    for item in items_list:
        comp_img_urls = []
        for url in item["item_urls"]:
            comp_img = await compress_image(url)
            comp_img_url = f"/processed_img/{uuid4()}.jpeg"

            # Using S3 Bucket to generate publicly accessible URL (Optional)
            comp_img_url = AWSS3().upload_file_aws(comp_img, comp_img_url)
            comp_img_urls.append(comp_img_url)
        
        # Insert processed image data into the database
        await items_coll.insert_one({
            "_id": str(uuid4()),
            "request_id": request_id,
            "created_at": time.time(),
            "updated_at": time.time(),
            "input_urls": item["item_urls"],
            "serial_no": item["serial_no"],
            "item_name": item["item_name"],
            "output_urls": comp_img_urls
        })
    
    # Update the request status to "Completed"
    await requests_coll.update_one({"_id": request_id}, {"$set": {"status": "Completed", "updated_at": time.time()}})

@app.post("/upload", response_model=UploadResponse)
async def upload_csv_api(csv_file: UploadFile, background_task: BackgroundTasks) -> JSONResponse:
    """
    API endpoint to upload a CSV file and start the background task for image processing
    """
    file_content = await csv_file.read()
    request_id = str(uuid4())

    # Insert initial request data into the database
    await requests_coll.insert_one({
        "_id": request_id,
        "status": "Pending",
        "created_at": time.time(),
        "updated_at": time.time()
    })

    # Save the uploaded CSV file to disk
    async with aiofiles.open(f"{request_id}.csv", "wb") as csv_file:
        await csv_file.write(file_content)

    items_list = []

    # Read and parse the CSV file
    async with aiofiles.open(f"{request_id}.csv", "r") as csv_file:
        content_dict = csv.DictReader(csv_file)
        for row in content_dict:
            items_list.append({
                "serial_no": row.get("S. No."),
                "item_name": row.get("Product Name"),
                "item_urls": row.get("Input Image Urls", "").split(",")
            })

    # Update the request status to "Processing"
    await requests_coll.update_one({"_id": request_id}, {"$set": {"status": "Processing", "updated_at": time.time()}})

    # Add the image processing task to the background tasks
    background_task.add_task(process_image, request_id, items_list)
    
    return JSONResponse({"request_id": request_id}, status_code=HTTP_200_OK)

@app.get("/status/{request_id}", response_model=StatusResponse)
async def get_image_processing_status_api(request_id: str) -> JSONResponse:
    """
    API endpoint to get the status of an image processing request by request ID
    """
    request_obj = await requests_coll.find_one({"_id": request_id})
    if not request_obj:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="Request not found")
    
    return JSONResponse({"request_id": request_id, "status": request_obj["status"]})

if __name__ == "__main__":
    uvicorn.run("main:app")
