import time
from typing import List
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
import aiohttp, aiofiles
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

app = FastAPI()

db_url = "mongodb://localhost:27017"
client = AsyncIOMotorClient(db_url)
db = client.process_image
requests_coll = db["requests"]
items_coll = db["items"]


class UploadResponse(BaseModel):
    request_id: str


class StatusResponse(BaseModel):
    request_id: str
    status: str
    compressed_images: list


class AWSS3:

    def __init__(self) -> None:
        self.S3_BUCKET = ""
        self.S3_KEY = ""
        self.S3_SECRET = ""
        self.S3_LOCATION = ""
        self.REGION_NAME = ""

    def upload_file_aws(self, byte_data, file_name):
        try:
            client = boto3.client('s3', region_name=self.REGION_NAME, aws_access_key_id=self.S3_KEY, aws_secret_access_key=self.S3_SECRET)
            
            fields = {'acl': 'public-read'}
            conditions = [{"acl": "public-read"}]

            fields['Content-Type'] = "image/png"
            conditions.append({"Content-Type": "image/png"})
                
            buc_address = "Bucket Address"
            boto3_response = client.generate_presigned_post(Bucket=self.S3_BUCKET, Key=buc_address, Fields=fields,
                                                            ExpiresIn=60, Conditions=conditions)
            
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
    for item in items_list:
        comp_img_urls = []
        for url in item["item_urls"]:
            comp_img = await compress_image(url)
            comp_img_url = f"/processed_img/{uuid4()}.jpeg"


            # async with aiofiles.open(comp_img_url, "wb") as output:
            #     output.write(comp_img)

            ## Using S3 Bucket to generate publicly accessable URL (Optional)
            comp_img_url = AWSS3().upload_file_aws(comp_img, comp_img_url)
            comp_img_urls.append(comp_img_url)
        await items_coll.insert_one({"_id": str(uuid4()),
                                     "request_id": request_id,
                                     "created_at": time.time(),
                                     "updated_at": time.time(),
                                     "input_urls": item["item_urls"],
                                     "serial_no": item["serial_no"],
                                     "item_name": item["item_name"],
                                     "output_urls": comp_img_urls})
        
    await requests_coll.update_one({"_id": request_id}, {"$set": {"status": "Completed", "updated_at": time.time()}})


@app.post("/upload", response_model = UploadResponse)
async def uplooad_csv_api(csv_file: UploadFile, background_task: BackgroundTasks) -> JSONResponse:
    file_content = csv_file.read()

    request_id = str(uuid4())

    await requests_coll.insert_one({
        "_id": request_id,
        "status": "Pending",
        "created_at": time.time(),
        "updated_at": time.time()
    })

    async with aiofiles.open(f"{request_id}.csv", "wb") as csv_file:
        await csv_file.write(file_content)

    items_list = []

    async with aiofiles.open(f"{request_id}.csv", "r") as csv_file:
        content_dict = csv.DictReader(csv_file)
        for row in content_dict:
            items_list.append({
                "serial_no": row.get("S. No."),
                # "request_id": request_id,
                "item_name": row.get("Product Name"),
                "item_urls": row.get("Input Image Urls", "").split(",")
            })
        # await items_coll.insert_many(items_list)

    await requests_coll.update_one({"_id": request_id}, {"$set": {"status": "Processing",
                                                                  "updated_at": time.time()}})

    background_task.add_task(process_image, request_id, items_list)
    return JSONResponse({"request_id": request_id}, status_code=HTTP_200_OK)


@app.get("/status/{request_id}", response_model=StatusResponse)
async def get_image_processing_status_api(request_id: str) -> JSONResponse:
    request_obj = requests_coll.find_one({"_id": request_id})
    if not request_obj:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="Request not Found")
    
    return JSONResponse({"request_id": request_id, "status": request_obj["status"]})


if __name__ == "__main__":
    uvicorn.run("imageProcessing:app")
